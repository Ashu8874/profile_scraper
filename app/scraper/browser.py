"""
scraper/browser.py — Browser lifecycle: launch, session load/save/clear, stealth.
"""

import logging
import os
import random

from app.core.config import CONFIG, LEGACY_SESSION_PATH, SESSION_PATH

logger = logging.getLogger(__name__)

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
]


def session_exists() -> bool:
    return os.path.exists(SESSION_PATH) or os.path.exists(LEGACY_SESSION_PATH)


def _existing_session_path() -> str | None:
    if os.path.exists(SESSION_PATH):
        return SESSION_PATH
    if os.path.exists(LEGACY_SESSION_PATH):
        logger.info("Using legacy saved session path")
        return LEGACY_SESSION_PATH
    return None


def clear_session():
    cleared = False
    for path in (SESSION_PATH, LEGACY_SESSION_PATH):
        if os.path.exists(path):
            os.remove(path)
            cleared = True
    if cleared:
        logger.info("Stale session cleared")


async def save_session(context):
    os.makedirs(os.path.dirname(SESSION_PATH), exist_ok=True)
    await context.storage_state(path=SESSION_PATH)
    logger.info("Session saved")


async def _apply_stealth(context):
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'plugins',   { get: () => [1,2,3,4,5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
        window.chrome = { runtime: {} };
        const _query = window.navigator.permissions.query;
        window.navigator.permissions.query = (p) =>
            p.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : _query(p);
    """)


async def launch_browser(playwright):
    browser = await playwright.chromium.launch(
        headless=CONFIG.get("browser_headless", False),
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-infobars",
            "--disable-dev-shm-usage",
        ],
    )

    ctx_kwargs = dict(
        viewport={
            "width": random.randint(CONFIG.get("browser_width_min", 1200), CONFIG.get("browser_width_max", 1400)),
            "height": random.randint(CONFIG.get("browser_height_min", 700), CONFIG.get("browser_height_max", 900)),
        },
        user_agent=random.choice(_USER_AGENTS),
        locale=CONFIG.get("browser_locale", "en-US"),
        timezone_id=CONFIG.get("browser_timezone", "America/New_York"),
    )

    existing_session = _existing_session_path()
    if existing_session:
        logger.info("Reusing saved session")
        ctx_kwargs["storage_state"] = existing_session
    else:
        logger.info("No session found — fresh start")

    context = await browser.new_context(**ctx_kwargs)
    await _apply_stealth(context)
    return browser, context
