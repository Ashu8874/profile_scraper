"""
Compatibility exports for older imports.
The source of truth now lives in app.services.ai_parser.
"""

from app.services.ai_parser import (
    BLOCKED_INDICATORS,
    MIN_PAGE_TEXT_LENGTH,
    clean_json,
    is_empty_profile,
    is_valid_page_text,
    parse_with_ai,
)

__all__ = [
    "BLOCKED_INDICATORS",
    "MIN_PAGE_TEXT_LENGTH",
    "clean_json",
    "is_empty_profile",
    "is_valid_page_text",
    "parse_with_ai",
]
