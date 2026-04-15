"""
core/config.py — Centralised settings loaded from environment / .env file.
All other modules import from here — never from os.environ directly.
"""

import os
from dotenv import load_dotenv

load_dotenv()


def _int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, default))
    except (ValueError, TypeError):
        return default


def _bool(key: str, default: bool) -> bool:
    return os.getenv(key, str(default)).lower() in ("true", "1", "yes")


def _csv_list(key: str, default: list[str]) -> list[str]:
    raw = os.getenv(key, "")
    if not raw.strip():
        return default
    items = [item.strip() for item in raw.split(",")]
    return [item for item in items if item]


def _endpoint_url(key: str, default: str) -> str:
    raw = os.getenv(key, default).strip()
    if not raw:
        return default
    if "://" in raw:
        return raw

    host = raw.split("/", 1)[0].lower()
    if host in {"localhost", "127.0.0.1"} or host.startswith(("192.168.", "10.", "172.")):
        return f"http://{raw}"
    return f"https://{raw}"


# ─── LinkedIn ─────────────────────────────────────────────────────────────────

LINKEDIN_EMAIL    = os.getenv("LINKEDIN_EMAIL", "")
LINKEDIN_PASSWORD = os.getenv("LINKEDIN_PASSWORD", "")

SEARCH_QUERIES = _csv_list("SEARCH_QUERIES", [
    "learn and development"
])

# ─── Scraper ──────────────────────────────────────────────────────────────────

MAX_PROFILES_PER_RUN = _int("MAX_PROFILES_PER_RUN", 20)
MAX_SEARCH_PAGES     = _int("MAX_SEARCH_PAGES", 10)
MIN_DELAY            = _int("MIN_DELAY", 8)
MAX_DELAY            = _int("MAX_DELAY", 20)

# ─── AI / Ollama ──────────────────────────────────────────────────────────────

OLLAMA_ENDPOINT = _endpoint_url("OLLAMA_ENDPOINT", "http://192.168.1.27:11434/api/generate")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL", "llama3.1:8b")

# ─── MongoDB ─────────────────────────────────────────────────────────────────

MONGODB_URI                         = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE                    = os.getenv("MONGODB_DATABASE", "linkedin_ai_scraper")
MONGODB_COLLECTION                  = os.getenv("MONGODB_COLLECTION", "profiles")
MONGODB_MAX_POOL_SIZE               = _int("MONGODB_MAX_POOL_SIZE", 20)
MONGODB_MIN_POOL_SIZE               = _int("MONGODB_MIN_POOL_SIZE", 0)
MONGODB_SERVER_SELECTION_TIMEOUT_MS = _int("MONGODB_SERVER_SELECTION_TIMEOUT_MS", 5000)
MONGODB_CONNECT_TIMEOUT_MS          = _int("MONGODB_CONNECT_TIMEOUT_MS", 5000)

# ─── Scheduler ────────────────────────────────────────────────────────────────

SCHEDULER_ENABLED   = _bool("SCHEDULER_ENABLED", True)
RUNS_PER_DAY        = _int("RUNS_PER_DAY", 5)
START_HOUR          = _int("START_HOUR", 9)
END_HOUR            = _int("END_HOUR", 21)
JITTER_MINUTES      = _int("JITTER_MINUTES", 15)
MIN_GAP_MINUTES     = _int("MIN_GAP_MINUTES", 90)
TIMEZONE            = os.getenv("TIMEZONE", "Asia/Kolkata")

# ─── Paths ────────────────────────────────────────────────────────────────────

BASE_DIR     = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DATA_DIR     = os.path.join(BASE_DIR, "data")
OUTPUT_PATH  = os.path.join(DATA_DIR, "output", "profiles.json")
SESSION_PATH = os.path.join(DATA_DIR, "session", "linkedin_session.json")
DEBUG_DIR    = os.path.join(BASE_DIR, "debug")
LOGS_DIR     = os.path.join(BASE_DIR, "logs")

# ─── Legacy path compatibility ───────────────────────────────────────────────

LEGACY_OUTPUT_PATH  = os.path.join(BASE_DIR, "output", "profiles.json")
LEGACY_SESSION_PATH = os.path.join(BASE_DIR, "session", "linkedin_session.json")

# ─── Compatibility dictionaries ──────────────────────────────────────────────

CONFIG = {
    "email": LINKEDIN_EMAIL,
    "password": LINKEDIN_PASSWORD,
    "search_queries": SEARCH_QUERIES,
    "max_profiles": MAX_PROFILES_PER_RUN,
    "max_search_pages": MAX_SEARCH_PAGES,
    "min_delay": MIN_DELAY,
    "max_delay": MAX_DELAY,
}

SCHEDULER_CONFIG = {
    "enabled": SCHEDULER_ENABLED,
    "runs_per_day": RUNS_PER_DAY,
    "profiles_per_run": MAX_PROFILES_PER_RUN,
    "start_hour": START_HOUR,
    "end_hour": END_HOUR,
    "jitter_minutes": JITTER_MINUTES,
    "min_gap_minutes": MIN_GAP_MINUTES,
    "timezone": TIMEZONE,
}
