"""
main.py — FastAPI app with built-in scrape scheduler.

Endpoints:
  GET /              — health check
  GET /stats         — stored profile counts
  GET /scrape        — manual scrape trigger
  GET /scheduler     — scheduler status + job history
  POST /scheduler/start  — start the scheduler
  POST /scheduler/stop   — stop the scheduler
  POST /scheduler/trigger — run an immediate job outside the schedule
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from app.core.config import SCHEDULER_ENABLED
from app.core.logging import setup_logging
from app.services.scheduler import ScraperScheduler
from app.services.storage import (
    close_storage,
    get_stats,
    init_storage,
    purge_non_success_profiles,
)
from app.scraper import run_scraper

# ─── Logging ──────────────────────────────────────────────────────────────────

setup_logging()
logger = logging.getLogger(__name__)

# ─── Scheduler singleton ──────────────────────────────────────────────────────

scheduler = ScraperScheduler()


# ─── Lifespan (startup / shutdown) ───────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_storage()

    if SCHEDULER_ENABLED:
        scheduler.start()
        logger.info("Scheduler started on app startup")
    else:
        logger.info("Scheduler disabled — manual /scrape only")

    yield

    # Shutdown
    scheduler.stop()
    await close_storage()
    logger.info("Scheduler stopped on app shutdown")


# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="LinkedIn AI Scraper",
    description="Scrapes LinkedIn profiles with a built-in daily scheduler.",
    version="2.0.0",
    lifespan=lifespan,
)


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def home():
    return {"status": "running", "version": "2.0.0"}


@app.get("/stats", tags=["Data"])
async def stats():
    """Summary of all stored profiles."""
    return await get_stats()


@app.post("/data/cleanup", tags=["Data"])
async def cleanup_non_success_profiles():
    """Delete stored profiles that are not marked as success."""
    deleted = await purge_non_success_profiles()
    return {"status": "success", "deleted": deleted}


@app.get("/scrape", tags=["Scrape"])
async def scrape():
    """
    Manually trigger a scrape run immediately.
    Respects deduplication — only scrapes new profiles.
    """
    try:
        result = await run_scraper()
        logger.info(
            f"Manual scrape complete — new: {result['newly_scraped']}, "
            f"skipped: {result['skipped_duplicates']}"
        )
        return {
            "status":            "success",
            "newly_scraped":     result["newly_scraped"],
            "skipped_duplicates": result["skipped_duplicates"],
            "data":              result["data"],
        }
    except RuntimeError as e:
        logger.error(f"Scraper error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception:
        logger.exception("Unexpected error during manual scrape")
        raise HTTPException(status_code=500, detail="Internal scraper error")


@app.get("/scheduler", tags=["Scheduler"])
def scheduler_status():
    """
    Returns scheduler state, today's job count, next scheduled run,
    and the last 10 job results.
    """
    return scheduler.get_status()


@app.post("/scheduler/start", tags=["Scheduler"])
def scheduler_start():
    """Start the scheduler if it's not already running."""
    scheduler.start()
    return {"status": "started"}


@app.post("/scheduler/stop", tags=["Scheduler"])
def scheduler_stop():
    """Stop the scheduler after the current job finishes."""
    scheduler.stop()
    return {"status": "stopped"}


@app.post("/scheduler/trigger", tags=["Scheduler"])
async def scheduler_trigger():
    """
    Immediately run a scrape job outside the normal schedule.
    Returns the job result synchronously.
    """
    try:
        result = await scheduler.trigger_now()
        return {"status": "success", "job": result}
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception:
        logger.exception("Unexpected error during triggered job")
        raise HTTPException(status_code=500, detail="Internal scraper error")
