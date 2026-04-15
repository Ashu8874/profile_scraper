"""
core/logging.py — Centralised logging setup.
Call setup_logging() once at app startup.
"""

import logging
import os
from app.core.config import LOGS_DIR


def setup_logging():
    os.makedirs(LOGS_DIR, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(os.path.join(LOGS_DIR, "scraper.log"), encoding="utf-8"),
        ],
    )
