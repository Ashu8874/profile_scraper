"""
Compatibility exports for older imports.
The source of truth now lives in app.services.storage.
"""

from app.services.storage import append_profile, get_scraped_urls, get_stats

__all__ = ["append_profile", "get_scraped_urls", "get_stats"]
