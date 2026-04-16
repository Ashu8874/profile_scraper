"""
core/logging.py — Centralised logging setup.
Call setup_logging() once at app startup.
"""

from datetime import datetime
import logging
import os
from zoneinfo import ZoneInfo

from app.core.config import LOGS_DIR, TIMEZONE


class TimezoneFormatter(logging.Formatter):
    def __init__(self, *args, timezone_name: str, **kwargs):
        super().__init__(*args, **kwargs)
        self._timezone = ZoneInfo(timezone_name)

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, self._timezone)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()


def setup_logging():
    os.makedirs(LOGS_DIR, exist_ok=True)

    formatter = TimezoneFormatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        timezone_name=TIMEZONE,
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(os.path.join(LOGS_DIR, "scraper.log"), encoding="utf-8")
    file_handler.setFormatter(formatter)

    logging.basicConfig(
        level=logging.INFO,
        handlers=[stream_handler, file_handler],
        force=True,
    )
