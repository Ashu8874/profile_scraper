"""
scraper.py — LinkedIn profile scraper with session reuse,
human behaviour simulation, and URL-based deduplication.
"""

import logging
from playwright.async_api import async_playwright
from app.config import CONFIG
from app.utils import (
    random_sleep,
    full_scroll,
    random_mouse_move,
    human_type,
    save_session,
    load_session,
    clear_session,
)
from app.ai_parser import parse_with_ai
from app.storage import get_scraped_urls, append_profile

logger = logging.getLogger(__name__)


# ─── Login ────────────────────────────────────────────────────────────────────

async def _is_logged_in(page) -> bool:
    """
    Navigate to /feed/ and wait for final redirect to settle.
    If LinkedIn bounces us to login, session is expired.
    """
    try:
        await page.goto("https://www.linkedin.com/feed/", wait_until="networkidle", timeout=20000)
    except Exception:
        # networkidle timeout is fine — just check the URL
        pass

    await random_sleep(2, 3)
    current = page.url
    logger.info(f"Session check URL: {current}")

    # Expired session redirects to /login or /uas/login
    if "login" in current or "authwall" in current or "uas" in current:
        logger.warning("Session expired — redirected to login page")
        return False

    return "feed" in current


async def _do_login(page, context):
    """Perform credential-based login."""
    logger.info("Navigating to login page...")
    await page.goto("https://www.linkedin.com/login", wait_until="networkidle", timeout=30000)
    await random_sleep(3, 5)

    current_url = page.url
    logger.info(f"Login page URL: {current_url}")

    # Take a screenshot so we can see what LinkedIn is actually showing
    import os
    os.makedirs("debug", exist_ok=True)
    await page.screenshot(path="debug/login_page.png", full_page=True)
    logger.info("Screenshot saved to debug/login_page.png")

    # Wait for login form — dismiss any overlays first
    try:
        # Dismiss cookie consent if present
        for consent_selector in [
            'button[action-type="ACCEPT"]',
            'button:has-text("Accept")',
            'button:has-text("Allow")',
        ]:
            try:
                btn = await page.query_selector(consent_selector)
                if btn:
                    await btn.click()
                    logger.info(f"Dismissed consent dialog: {consent_selector}")
                    await random_sleep(1, 2)
                    break
            except Exception:
                pass

        await page.wait_for_selector("#username", state="visible", timeout=45000)

    except Exception:
        # Save another screenshot after timeout to see current state
        await page.screenshot(path="debug/login_timeout.png", full_page=True)
        logger.error(
            f"Login form not found after 45s. URL: {page.url}. "
            "Check debug/login_page.png and debug/login_timeout.png"
        )
        raise RuntimeError(
            f"Login form not found. URL: {page.url}. "
            "LinkedIn may be showing a CAPTCHA — check debug/login_page.png"
        )

    # Clear fields first in case of pre-filled values
    await page.fill("#username", "")
    await page.fill("#password", "")
    await random_sleep(0.5, 1.0)

    await human_type(page, "#username", CONFIG["email"])
    await random_sleep(0.5, 1.5)
    await human_type(page, "#password", CONFIG["password"])
    await random_sleep(0.8, 1.8)

    await random_mouse_move(page)
    await page.click('[type="submit"]')

    # Wait for redirect — feed, checkpoint, or error
    try:
        await page.wait_for_function(
            """() => {
                const url = window.location.href;
                return url.includes('feed') ||
                       url.includes('checkpoint') ||
                       url.includes('challenge') ||
                       url.includes('error');
            }""",
            timeout=60000
        )
    except Exception:
        await page.screenshot(path="debug/after_submit.png", full_page=True)
        raise RuntimeError("Login timed out — no redirect after submitting credentials")

    current_url = page.url

    if "feed" in current_url:
        logger.info("Login successful")
        await save_session(context)
        return

    if "checkpoint" in current_url or "challenge" in current_url:
        logger.warning("LinkedIn security checkpoint — please complete in browser")
        logger.warning(f"URL: {current_url}")
        await page.wait_for_function(
            "() => window.location.href.includes('feed')",
            timeout=120000  # 2 min for manual verification
        )
        logger.info("Manual verification completed")
        await save_session(context)
        return

    await page.screenshot(path="debug/login_failed.png", full_page=True)
    raise RuntimeError(f"Login failed — unexpected URL: {current_url}")


async def login(page, context):
    """
    Smart login with session validation.
    - If saved session exists, verify it's still valid
    - If session is stale/expired, clear it and re-login
    - Handles checkpoints and CAPTCHA with manual fallback
    """
    logger.info("Checking login state...")

    logged_in = await _is_logged_in(page)

    if logged_in:
        logger.info("Session valid — already logged in")
        return

    # Session was loaded but is expired/invalid — clear it
    logger.warning("Saved session is stale or expired — clearing and re-logging in")
    clear_session()

    await _do_login(page, context)


# ─── Collect enough NEW profile links (scroll + next-page pagination) ────────

async def _extract_links_from_page(page) -> list[str]:
    """Extract all /in/ profile links visible on current page."""
    return await page.evaluate("""
        () => {
            return [...document.querySelectorAll('a[href]')]
                .map(a => a.href.split("?")[0].trim())
                .filter(h =>
                    h.startsWith("https://www.linkedin.com/in/") &&
                    h.replace("https://www.linkedin.com/in/", "").replace("/", "").length > 1
                );
        }
    """)


async def _go_to_next_page(page) -> bool:
    """
    Click LinkedIn's Next button to go to the next search results page.
    Tries multiple known selectors since LinkedIn changes them frequently.
    Returns True if navigation succeeded, False if no Next button found.
    """
    # LinkedIn uses several different next-button patterns
    next_selectors = [
        'button[aria-label="Next"]',
        'button.artdeco-pagination__button--next',
        '[data-test-pagination-page-btn="next"]',
        'li.artdeco-pagination__indicator--number:last-child button',
    ]

    next_btn = None
    for selector in next_selectors:
        try:
            btn = await page.query_selector(selector)
            if btn:
                next_btn = btn
                logger.info(f"Next button found via: {selector}")
                break
        except Exception:
            continue

    if not next_btn:
        # Last resort: find by button text content
        try:
            next_btn = await page.query_selector("button:has-text('Next')")
        except Exception:
            pass

    if not next_btn:
        logger.info("No 'Next' button found — reached last search page")
        return False

    try:
        is_disabled = await next_btn.get_attribute("disabled")
        if is_disabled is not None:
            logger.info("'Next' button is disabled — no more pages")
            return False

        await next_btn.scroll_into_view_if_needed()
        await random_sleep(1, 2)

        current_url = page.url
        await next_btn.click()

        # Wait until URL changes or new results load
        await random_sleep(3, 5)
        await page.wait_for_load_state("domcontentloaded")
        await random_sleep(2, 3)

        new_url = page.url
        logger.info(f"Navigated to next page — URL: {new_url}")
        return True

    except Exception as e:
        logger.warning(f"Next page click failed: {e}")
        return False


async def collect_new_links(page, required: int) -> tuple[list[str], int]:
    """
    Collect `required` number of NEW (not yet scraped) profile URLs.
    Rotates through all configured search queries if one is exhausted.
    Uses Next button for pagination within each query.
    """
    scraped_urls  = get_scraped_urls()
    new_links:    list[str] = []
    skipped_count = 0
    max_pages     = CONFIG.get("max_search_pages", 10)
    queries       = CONFIG.get("search_queries", [CONFIG.get("search_query", "developer")])

    for query in queries:
        if len(new_links) >= required:
            break

        encoded_query = query.replace(" ", "%20")
        search_url    = (
            f"https://www.linkedin.com/search/results/people/"
            f"?keywords={encoded_query}"
        )
        logger.info(f"Searching query: '{query}'")
        await page.goto(search_url, wait_until="domcontentloaded")
        await random_sleep(3, 5)

        page_num = 1
        while len(new_links) < required and page_num <= max_pages:

            # Guard: session expired
            if "login" in page.url or "authwall" in page.url or "uas" in page.url:
                logger.error(f"Session expired during search — URL: {page.url}")
                raise RuntimeError("Session expired during search — restart to re-login")

            logger.info(f"Query '{query}' — page {page_num} — URL: {page.url}")

            await full_scroll(page)
            await random_mouse_move(page)
            await random_sleep(2, 3)

            raw_links      = await _extract_links_from_page(page)
            unique_on_page = list(dict.fromkeys(raw_links))
            logger.info(f"  {len(unique_on_page)} unique links on page {page_num}")

            before = len(new_links)
            for url in unique_on_page:
                if url in scraped_urls:
                    skipped_count += 1
                elif url not in new_links:
                    new_links.append(url)
                    if len(new_links) >= required:
                        break

            added = len(new_links) - before
            logger.info(
                f"  +{added} new | {len(new_links)}/{required} total | "
                f"{skipped_count} skipped"
            )

            if len(new_links) >= required:
                break

            navigated = await _go_to_next_page(page)
            if not navigated:
                logger.info(f"Query '{query}' exhausted after {page_num} page(s)")
                break

            page_num += 1

    if len(new_links) < required:
        logger.warning(
            f"Collected {len(new_links)}/{required} new profiles "
            f"after exhausting all queries"
        )

    return new_links[:required], skipped_count


# ─── Scrape single profile ────────────────────────────────────────────────────

async def scrape_profile(page, url: str) -> dict:
    await page.goto(url, wait_until="domcontentloaded")
    await random_sleep(4, 8)

    await random_mouse_move(page)
    await full_scroll(page)
    await random_sleep(2, 5)
    await random_mouse_move(page)
    await random_sleep(1, 3)

    text: str = await page.evaluate("() => document.body.innerText")
    data: dict = parse_with_ai(text)
    return data


# ─── Main runner ──────────────────────────────────────────────────────────────

async def run_scraper() -> dict:
    """
    Main entry point. Returns a summary dict with counts and newly scraped data.
    """
    newly_scraped = []

    async with async_playwright() as p:
        browser, context = await load_session(p)
        page = await context.new_page()

        try:
            await login(page, context)

            new_links, skipped_count = await collect_new_links(page, required=CONFIG["max_profiles"])

            if not new_links:
                logger.info("No new profiles to scrape — all already stored")
            else:
                logger.info(f"Scraping {len(new_links)} new profile(s)...")

            for i, url in enumerate(new_links):
                logger.info(f"[{i+1}/{len(new_links)}] Scraping: {url}")

                try:
                    data = await scrape_profile(page, url)
                    profile = {**data, "url": url}

                    saved = append_profile(profile)  # writes to disk immediately

                    if saved:
                        newly_scraped.append(profile)
                    else:
                        logger.warning(f"Profile not saved (parse error or duplicate): {url}")

                except Exception as e:
                    logger.error(f"Failed to scrape {url}: {e}")

                await random_sleep(CONFIG["min_delay"], CONFIG["max_delay"])

            await save_session(context)

        finally:
            await browser.close()

    return {
        "newly_scraped": len(newly_scraped),
        "skipped_duplicates": skipped_count,
        "data": newly_scraped,
    }
