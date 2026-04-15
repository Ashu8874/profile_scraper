"""
services/scheduler.py — Daily scrape scheduler with jitter and job history.
"""

import asyncio
import logging
import random
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional
from zoneinfo import ZoneInfo
from app.core.config import (
    RUNS_PER_DAY, START_HOUR, END_HOUR,
    JITTER_MINUTES, MIN_GAP_MINUTES, TIMEZONE,
    MAX_PROFILES_PER_RUN,
)

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    COMPLETED = "completed"
    FAILED    = "failed"
    SKIPPED   = "skipped"


class ScrapeJob:
    def __init__(self, scheduled_at: datetime):
        self.scheduled_at  = scheduled_at
        self.started_at:  Optional[datetime] = None
        self.finished_at: Optional[datetime] = None
        self.status        = JobStatus.PENDING
        self.result:      Optional[dict]     = None
        self.error:       Optional[str]      = None

    def to_dict(self) -> dict:
        return {
            "scheduled_at":  self.scheduled_at.isoformat(),
            "started_at":    self.started_at.isoformat()  if self.started_at  else None,
            "finished_at":   self.finished_at.isoformat() if self.finished_at else None,
            "status":        self.status,
            "newly_scraped": self.result.get("newly_scraped") if self.result else None,
            "skipped":       self.result.get("skipped_duplicates") if self.result else None,
            "error":         self.error,
        }


class ScraperScheduler:
    def __init__(self):
        self.tz             = ZoneInfo(TIMEZONE)
        self.jobs:          list[ScrapeJob]      = []
        self._task:         Optional[asyncio.Task] = None
        self._stop_event    = asyncio.Event()
        self._running_lock  = asyncio.Lock()

    # ── Schedule building ─────────────────────────────────────────────────────

    def _build_schedule(self) -> list[datetime]:
        now        = datetime.now(self.tz)
        window_min = (END_HOUR - START_HOUR) * 60
        slot_min   = window_min // RUNS_PER_DAY
        times:     list[datetime] = []

        for i in range(RUNS_PER_DAY):
            base      = now.replace(hour=START_HOUR, minute=0, second=0, microsecond=0)
            base     += timedelta(minutes=i * slot_min)
            jitter    = random.randint(-JITTER_MINUTES, JITTER_MINUTES)
            scheduled = base + timedelta(minutes=jitter)

            if times:
                min_allowed = times[-1] + timedelta(minutes=MIN_GAP_MINUTES)
                if scheduled < min_allowed:
                    scheduled = min_allowed

            if scheduled > now:
                times.append(scheduled)

        logger.info(
            f"[Scheduler] Today's schedule ({len(times)} runs): "
            + ", ".join(t.strftime("%H:%M") for t in times)
        )
        return times

    # ── Execution ─────────────────────────────────────────────────────────────

    async def _execute_job(self, job: ScrapeJob):
        from app.scraper.runner import run_scraper

        async with self._running_lock:
            job.status     = JobStatus.RUNNING
            job.started_at = datetime.now(self.tz)
            logger.info(f"[Scheduler] Job started (scheduled: {job.scheduled_at.strftime('%H:%M')})")

            try:
                result          = await run_scraper()
                job.result      = result
                job.status      = JobStatus.COMPLETED
                job.finished_at = datetime.now(self.tz)
                duration        = (job.finished_at - job.started_at).seconds
                logger.info(
                    f"[Scheduler] Completed in {duration}s — "
                    f"new: {result['newly_scraped']}, skipped: {result['skipped_duplicates']}"
                )
            except Exception as e:
                job.status      = JobStatus.FAILED
                job.error       = str(e)
                job.finished_at = datetime.now(self.tz)
                logger.error(f"[Scheduler] Job failed: {e}")

    async def _wait_until(self, target: datetime):
        while True:
            remaining = (target - datetime.now(self.tz)).total_seconds()
            if remaining <= 0 or self._stop_event.is_set():
                return
            await asyncio.sleep(min(remaining, 30))

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _run_loop(self):
        logger.info("[Scheduler] Loop started")

        while not self._stop_event.is_set():
            schedule = self._build_schedule()

            if not schedule:
                tomorrow = (datetime.now(self.tz) + timedelta(days=1)).replace(
                    hour=0, minute=1, second=0, microsecond=0
                )
                logger.info(f"[Scheduler] No slots left today — sleeping until {tomorrow.strftime('%Y-%m-%d %H:%M')}")
                await self._wait_until(tomorrow)
                continue

            for run_time in schedule:
                if self._stop_event.is_set():
                    break
                job = ScrapeJob(scheduled_at=run_time)
                self.jobs.append(job)
                logger.info(f"[Scheduler] Next run at {run_time.strftime('%H:%M %Z')}")
                await self._wait_until(run_time)
                if self._stop_event.is_set():
                    job.status = JobStatus.SKIPPED
                    break
                await self._execute_job(job)

            if not self._stop_event.is_set():
                midnight = (datetime.now(self.tz) + timedelta(days=1)).replace(
                    hour=0, minute=1, second=0, microsecond=0
                )
                logger.info("[Scheduler] All jobs done — rebuilding schedule at midnight")
                await self._wait_until(midnight)

        logger.info("[Scheduler] Stopped")

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self):
        if self._task and not self._task.done():
            logger.warning("[Scheduler] Already running")
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())
        logger.info("[Scheduler] Started")

    def stop(self):
        self._stop_event.set()
        logger.info("[Scheduler] Stop requested")

    def get_status(self) -> dict:
        now      = datetime.now(self.tz)
        next_job = next((j for j in self.jobs if j.status == JobStatus.PENDING), None)
        return {
            "active":            not self._stop_event.is_set(),
            "currently_running": self._running_lock.locked(),
            "next_run":          next_job.scheduled_at.isoformat() if next_job else None,
            "timezone":          TIMEZONE,
            "runs_per_day":      RUNS_PER_DAY,
            "profiles_per_run":  MAX_PROFILES_PER_RUN,
            "jobs_today":        sum(1 for j in self.jobs if j.scheduled_at.date() == now.date()),
            "recent_jobs":       [j.to_dict() for j in self.jobs[-10:]],
        }

    async def trigger_now(self) -> dict:
        if self._running_lock.locked():
            raise RuntimeError("A scrape job is already running")
        job = ScrapeJob(scheduled_at=datetime.now(self.tz))
        self.jobs.append(job)
        await self._execute_job(job)
        return job.to_dict()
