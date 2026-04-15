import asyncio
import random
import os
import json


# ─── Delays ───────────────────────────────────────────────────────────────────

async def sleep(seconds: float):
    """Fixed sleep."""
    await asyncio.sleep(seconds)


async def random_sleep(min_sec: float = 2.0, max_sec: float = 6.0):
    """Human-like random delay."""
    delay = random.uniform(min_sec, max_sec)
    await asyncio.sleep(delay)


# ─── Scrolling ────────────────────────────────────────────────────────────────

async def full_scroll(page):
    """Scroll like a human — variable speed, random pauses, occasional scroll-up."""
    scroll_steps = random.randint(4, 8)

    for i in range(scroll_steps):
        scroll_amount = random.randint(400, 900)
        await page.mouse.wheel(0, scroll_amount)
        await asyncio.sleep(random.uniform(0.5, 2.0))

        # Occasionally scroll back up a little (human behaviour)
        if random.random() < 0.25:
            await page.mouse.wheel(0, -random.randint(100, 300))
            await asyncio.sleep(random.uniform(0.3, 1.0))


# ─── Mouse movement ───────────────────────────────────────────────────────────

async def random_mouse_move(page):
    """Move mouse to random positions to simulate human presence."""
    viewport = page.viewport_size or {"width": 1280, "height": 800}
    moves = random.randint(2, 5)

    for _ in range(moves):
        x = random.randint(100, viewport["width"] - 100)
        y = random.randint(100, viewport["height"] - 100)
        await page.mouse.move(x, y)
        await asyncio.sleep(random.uniform(0.1, 0.4))


# ─── Typing ───────────────────────────────────────────────────────────────────

async def human_type(page, selector: str, text: str):
    """Type into a field character-by-character with random delays."""
    await page.click(selector)
    await asyncio.sleep(random.uniform(0.3, 0.7))

    for char in text:
        await page.type(selector, char, delay=random.randint(60, 180))
        # Occasional micro-pause (like thinking)
        if random.random() < 0.1:
            await asyncio.sleep(random.uniform(0.2, 0.5))


# ─── Session persistence ──────────────────────────────────────────────────────

SESSION_PATH = "session/linkedin_session.json"


def session_exists() -> bool:
    return os.path.exists(SESSION_PATH)


async def save_session(context):
    """Save browser storage state to disk."""
    os.makedirs(os.path.dirname(SESSION_PATH), exist_ok=True)
    await context.storage_state(path=SESSION_PATH)
    print("💾 Session saved")


def clear_session():
    """Delete saved session so next run starts fresh."""
    if os.path.exists(SESSION_PATH):
        os.remove(SESSION_PATH)
        print("🗑️  Stale session cleared")


async def load_session(playwright):
    """Launch browser and load existing session if available."""
    from app.config import CONFIG

    browser = await playwright.chromium.launch(
        headless=False,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-infobars",
            "--disable-dev-shm-usage",
        ]
    )

    if session_exists():
        print("♻️  Reusing saved session")
        context = await browser.new_context(
            storage_state=SESSION_PATH,
            viewport={"width": random.randint(1200, 1400), "height": random.randint(700, 900)},
            user_agent=random_user_agent(),
            locale="en-US",
            timezone_id="America/New_York",
        )
    else:
        print("🆕 No session found — fresh start")
        context = await browser.new_context(
            viewport={"width": random.randint(1200, 1400), "height": random.randint(700, 900)},
            user_agent=random_user_agent(),
            locale="en-US",
            timezone_id="America/New_York",
        )

    await _apply_stealth_scripts(context)
    return browser, context


async def _apply_stealth_scripts(context):
    """Inject JS to mask automation fingerprints."""
    await context.add_init_script("""
        // Remove webdriver flag
        Object.defineProperty(navigator, 'webdriver', { get: () => false });

        // Fake plugins
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5],
        });

        // Fake languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en'],
        });

        // Spoof chrome runtime
        window.chrome = { runtime: {} };

        // Permissions spoof
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) =>
            parameters.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : originalQuery(parameters);
    """)


# ─── User-Agent rotation ──────────────────────────────────────────────────────

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
]


def random_user_agent() -> str:
    return random.choice(_USER_AGENTS)
