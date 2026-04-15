"""
Compatibility exports for older imports.
The source of truth now lives in app.services.scheduler.
"""

from app.services.scheduler import JobStatus, ScrapeJob
from app.services.scheduler import ScraperScheduler as _ServiceScraperScheduler


class ScraperScheduler(_ServiceScraperScheduler):
    def __init__(self, config: dict | None = None):
        super().__init__()


__all__ = ["JobStatus", "ScrapeJob", "ScraperScheduler"]
