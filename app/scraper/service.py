"""
scraper/service.py — LinkedIn scraping flow using the refactored modules.
"""

import asyncio
import logging
import os
import time
from urllib.parse import quote

from playwright.async_api import async_playwright

from app.core.config import (
    CONFIG,
    DEBUG_DIR,
    LOGIN_FORM_TIMEOUT_SEC,
    LOGIN_PAGE_TIMEOUT_SEC,
    MANUAL_VERIFICATION_TIMEOUT_SEC,
)
from app.scraper.browser import clear_session, launch_browser, save_session
from app.scraper.human import full_scroll, human_type, random_mouse_move, random_sleep
from app.services.ai_parser import parse_with_ai
from app.services.storage import (
    extract_profile_key,
    get_existing_success_profile_keys,
    init_storage,
    touch_profiles_for_keyword,
    upsert_profile,
)

logger = logging.getLogger(__name__)

_EMAIL_SELECTORS = [
    "#username",
    'input[name="session_key"]',
    'input[autocomplete="username"]',
    'input[type="email"]',
    'input[name="username"]',
]

_PASSWORD_SELECTORS = [
    "#password",
    'input[name="session_password"]',
    'input[autocomplete="current-password"]',
    'input[type="password"]',
]

_SUBMIT_SELECTORS = [
    'button[type="submit"]',
    'button:has-text("Sign in")',
    'button:has-text("Log in")',
    'input[type="submit"]',
]

_CONSENT_SELECTORS = [
    'button[action-type="ACCEPT"]',
    'button:has-text("Accept")',
    'button:has-text("Allow")',
    'button:has-text("I agree")',
]


def _is_auth_redirect(url: str) -> bool:
    lowered = url.lower()
    return "login" in lowered or "authwall" in lowered or "uas" in lowered


def _is_retryable_profile_error(message: str) -> bool:
    lowered = (message or "").lower()
    return any(
        token in lowered
        for token in (
            "page text invalid",
            "auth wall",
            "sign in",
            "empty profile",
            "invalid json from ai",
            "ai request failed",
            "bad gateway",
            "temporarily unavailable",
        )
    )


def _is_retryable_runtime_error(exc: Exception) -> bool:
    lowered = str(exc).lower()
    return any(
        token in lowered
        for token in (
            "timeout",
            "target page, context or browser has been closed",
            "execution context was destroyed",
            "net::",
            "connection reset",
        )
    )


def _search_url_for_query(query: str, page_num: int = 1) -> str:
    base = f"https://www.linkedin.com/search/results/people/?keywords={quote(query)}"
    if page_num > 1:
        return f"{base}&page={page_num}"
    return base


async def _goto_allowing_partial_load(page, url: str, *, timeout: int = 30000) -> None:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        return
    except Exception as exc:
        current = page.url
        if "Timeout" in str(exc) and current.startswith("https://www.linkedin.com/"):
            logger.warning(
                "Navigation to %s timed out after DOM load wait. Continuing on current URL: %s",
                url,
                current,
            )
            return
        raise


async def _evaluate_with_navigation_retry(page, script: str, *, attempts: int = 3):
    last_exc = None

    for attempt in range(1, attempts + 1):
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=10000)
        except Exception:
            pass

        try:
            return await page.evaluate(script)
        except Exception as exc:
            last_exc = exc
            message = str(exc)
            if "Execution context was destroyed" not in message:
                raise

            logger.warning(
                "DOM read interrupted by navigation (attempt %s/%s) — URL: %s",
                attempt,
                attempts,
                page.url,
            )

            if _is_auth_redirect(page.url):
                raise RuntimeError("LinkedIn redirected away while reading the page") from exc

            if attempt == attempts:
                break

            await random_sleep(1, 2)

    if last_exc is not None:
        raise last_exc

    raise RuntimeError("DOM evaluation failed unexpectedly")


async def _is_logged_in(page) -> bool:
    try:
        await _goto_allowing_partial_load(page, "https://www.linkedin.com/feed/", timeout=20000)
    except Exception:
        pass

    await random_sleep(2, 3)
    current = page.url
    logger.info("Session check URL: %s", current)

    if _is_auth_redirect(current):
        logger.warning("Session expired — redirected to login page")
        return False

    return "feed" in current


async def _page_text_snippet(page, limit: int = 2000) -> str:
    try:
        snippet = await _evaluate_with_navigation_retry(
            page,
            f"() => (document.body && document.body.innerText ? document.body.innerText : '').slice(0, {limit})",
        )
        return (snippet or "").strip()
    except Exception:
        return ""


async def _save_debug_artifacts(page, stem: str):
    os.makedirs(DEBUG_DIR, exist_ok=True)
    screenshot_path = os.path.join(DEBUG_DIR, f"{stem}.png")
    html_path = os.path.join(DEBUG_DIR, f"{stem}.html")
    text_path = os.path.join(DEBUG_DIR, f"{stem}.txt")

    try:
        await page.screenshot(path=screenshot_path, full_page=True)
        logger.info("Screenshot saved to %s", os.path.relpath(screenshot_path))
    except Exception as exc:
        logger.warning("Could not save screenshot %s: %s", screenshot_path, exc)

    try:
        html = await page.content()
        with open(html_path, "w", encoding="utf-8") as handle:
            handle.write(html)
    except Exception as exc:
        logger.warning("Could not save HTML debug artifact %s: %s", html_path, exc)

    try:
        text = await _page_text_snippet(page, limit=4000)
        with open(text_path, "w", encoding="utf-8") as handle:
            handle.write(text)
    except Exception as exc:
        logger.warning("Could not save text debug artifact %s: %s", text_path, exc)


async def _find_visible_selector(page, selectors: list[str]) -> str | None:
    for selector in selectors:
        try:
            element = await page.query_selector(selector)
            if element and await element.is_visible():
                return selector
        except Exception:
            continue
    return None


async def _classify_login_page(page) -> tuple[str, str]:
    current_url = page.url
    try:
        title = await page.title()
    except Exception:
        title = ""
    snippet = await _page_text_snippet(page)
    combined = f"{title}\n{snippet}".lower()

    email_selector = await _find_visible_selector(page, _EMAIL_SELECTORS)
    password_selector = await _find_visible_selector(page, _PASSWORD_SELECTORS)
    if email_selector and password_selector:
        return "login_form", "Login fields are visible"

    if any(token in current_url.lower() for token in ("checkpoint", "challenge")):
        return "challenge", "LinkedIn is showing a security checkpoint page"

    if any(token in combined for token in (
        "captcha",
        "verify you are human",
        "security verification",
        "puzzle",
        "let's do a quick security check",
    )):
        return "captcha", "LinkedIn is showing a CAPTCHA or human verification step"

    if any(token in combined for token in ("sign in", "login", "welcome back")):
        return "login_without_inputs", "Login page loaded without detectable username/password fields"

    return "unknown", "LinkedIn returned an unexpected login/interstitial page"


async def _dismiss_consent_dialogs(page):
    for consent_selector in _CONSENT_SELECTORS:
        try:
            button = await page.query_selector(consent_selector)
            if button and await button.is_visible():
                await button.click()
                logger.info("Dismissed consent dialog: %s", consent_selector)
                await random_sleep(1, 2)
                return
        except Exception:
            continue


async def _wait_for_login_form(page) -> tuple[str, str]:
    deadline = time.monotonic() + LOGIN_FORM_TIMEOUT_SEC

    while time.monotonic() < deadline:
        email_selector = await _find_visible_selector(page, _EMAIL_SELECTORS)
        password_selector = await _find_visible_selector(page, _PASSWORD_SELECTORS)
        if email_selector and password_selector:
            return email_selector, password_selector

        await asyncio.sleep(0.5)

    page_kind, details = await _classify_login_page(page)
    await _save_debug_artifacts(page, "login_timeout")
    raise RuntimeError(
        f"Login form not found after {LOGIN_FORM_TIMEOUT_SEC}s. URL: {page.url}. "
        f"Detected state: {page_kind} ({details}). Check debug/login_timeout.png, .html, and .txt"
    )


async def _click_login_submit(page):
    submit_selector = await _find_visible_selector(page, _SUBMIT_SELECTORS)
    if not submit_selector:
        raise RuntimeError("Login submit button not found on the page")

    await random_mouse_move(page)
    await page.click(submit_selector)


async def _can_access_search(page) -> bool:
    probe_query = CONFIG.get("search_queries", ["developer"])[0]
    probe_url = _search_url_for_query(probe_query)

    try:
        await _goto_allowing_partial_load(page, probe_url, timeout=30000)
        await random_sleep(2, 3)
    except Exception as exc:
        logger.warning("Search access probe failed: %s", exc)
        return False

    current = page.url
    logger.info("Search access probe URL: %s", current)
    return not _is_auth_redirect(current)


async def _do_login(page, context):
    logger.info("Navigating to login page...")
    await _goto_allowing_partial_load(
        page,
        "https://www.linkedin.com/login",
        timeout=LOGIN_PAGE_TIMEOUT_SEC * 1000,
    )
    await random_sleep(3, 5)

    current_url = page.url
    logger.info("Login page URL: %s", current_url)

    await _save_debug_artifacts(page, "login_page")

    try:
        await _dismiss_consent_dialogs(page)
        email_selector, password_selector = await _wait_for_login_form(page)
    except Exception as exc:
        logger.error(
            "Login form not found. URL: %s. Check debug artifacts.",
            page.url,
        )
        raise RuntimeError(str(exc)) from exc

    await page.fill(email_selector, "")
    await page.fill(password_selector, "")
    await random_sleep(0.5, 1.0)

    await human_type(page, email_selector, CONFIG["email"])
    await random_sleep(0.5, 1.5)
    await human_type(page, password_selector, CONFIG["password"])
    await random_sleep(0.8, 1.8)

    await _click_login_submit(page)

    try:
        await page.wait_for_function(
            """() => {
                const url = window.location.href;
                return url.includes('feed') ||
                       url.includes('checkpoint') ||
                       url.includes('challenge') ||
                       url.includes('error');
            }""",
            timeout=60000,
        )
    except Exception as exc:
        await _save_debug_artifacts(page, "after_submit")
        page_kind, details = await _classify_login_page(page)
        raise RuntimeError(
            "Login timed out after submitting credentials. "
            f"Detected state: {page_kind} ({details}). "
            "Check debug/after_submit.png, .html, and .txt"
        ) from exc

    current_url = page.url
    if "feed" in current_url:
        logger.info("Login successful")
        await save_session(context)
        return

    if "checkpoint" in current_url or "challenge" in current_url:
        logger.warning("LinkedIn security checkpoint — please complete in browser")
        logger.warning("URL: %s", current_url)
        await page.wait_for_function(
            "() => window.location.href.includes('feed')",
            timeout=MANUAL_VERIFICATION_TIMEOUT_SEC * 1000,
        )
        logger.info("Manual verification completed")
        await save_session(context)
        return

    await _save_debug_artifacts(page, "login_failed")
    page_kind, details = await _classify_login_page(page)
    raise RuntimeError(
        f"Login failed — unexpected URL: {current_url}. "
        f"Detected state: {page_kind} ({details}). "
        "Check debug/login_failed.png, .html, and .txt"
    )


async def login(page, context, force_reauth: bool = False):
    logger.info("Checking login state...")

    if not force_reauth and await _is_logged_in(page):
        if await _can_access_search(page):
            logger.info("Session valid — already logged in")
            return
        logger.warning("Feed is reachable but LinkedIn search requires fresh authentication")
    elif force_reauth:
        logger.warning("Forced re-authentication requested")

    logger.warning("Saved session is stale or expired — clearing and re-logging in")
    clear_session()
    await context.clear_cookies()
    await _do_login(page, context)

    if not await _can_access_search(page):
        raise RuntimeError("Login succeeded, but LinkedIn search still redirects to auth")


async def _extract_links_from_page(page) -> list[str]:
    return await _evaluate_with_navigation_retry(
        page,
        """
        () => {
            return [...document.querySelectorAll('a[href]')]
                .map(a => a.href.split("?")[0].trim())
                .filter(h =>
                    h.startsWith("https://www.linkedin.com/in/") &&
                    h.replace("https://www.linkedin.com/in/", "").replace("/", "").length > 1
                );
        }
        """,
    )


async def _search_page_looks_empty(page) -> bool:
    text = await _evaluate_with_navigation_retry(
        page,
        "() => (document.body && document.body.innerText ? document.body.innerText : '').slice(0, 4000)",
    )
    lower = text.lower()
    return any(
        marker in lower
        for marker in (
            "no results found",
            "try adjusting your search",
            "we couldn't find a match",
            "no matching people found",
        )
    )


async def _extract_links_with_retries(page, context, query: str, page_num: int) -> list[str]:
    for attempt in range(1, 4):
        raw_links = await _extract_links_from_page(page)
        unique_links = list(dict.fromkeys(raw_links))
        if unique_links:
            return unique_links

        if await _search_page_looks_empty(page):
            logger.info("Search page %s for '%s' appears genuinely empty", page_num, query)
            return []

        logger.warning(
            "No profile links found on query '%s' page %s (attempt %s/3) — retrying",
            query,
            page_num,
            attempt,
        )

        if attempt < 3:
            await random_sleep(2, 4)
            await _goto_search_results(page, context, query, page_num)

    return []


async def _go_to_next_page(page) -> bool:
    next_selectors = [
        'button[aria-label="Next"]',
        'button.artdeco-pagination__button--next',
        '[data-test-pagination-page-btn="next"]',
        'li.artdeco-pagination__indicator--number:last-child button',
    ]

    next_button = None
    for selector in next_selectors:
        try:
            button = await page.query_selector(selector)
            if button:
                next_button = button
                logger.info("Next button found via: %s", selector)
                break
        except Exception:
            continue

    if not next_button:
        try:
            next_button = await page.query_selector("button:has-text('Next')")
        except Exception:
            next_button = None

    if not next_button:
        logger.info("No 'Next' button found — reached last search page")
        return False

    try:
        if await next_button.get_attribute("disabled") is not None:
            logger.info("'Next' button is disabled — no more pages")
            return False

        await next_button.scroll_into_view_if_needed()
        await random_sleep(1, 2)
        await next_button.click()
        await random_sleep(3, 5)
        await page.wait_for_load_state("domcontentloaded")
        await random_sleep(2, 3)
        logger.info("Navigated to next page — URL: %s", page.url)
        return True
    except Exception as exc:
        logger.warning("Next page click failed: %s", exc)
        return False


async def _goto_search_results(page, context, query: str, page_num: int = 1):
    search_url = _search_url_for_query(query, page_num)
    logger.info("Opening query '%s' page %s", query, page_num)
    await _goto_allowing_partial_load(page, search_url, timeout=30000)
    await random_sleep(3, 5)

    if _is_auth_redirect(page.url):
        logger.warning(
            "LinkedIn redirected search query '%s' page %s to auth. Re-authenticating.",
            query,
            page_num,
        )
        await login(page, context, force_reauth=True)
        await _goto_allowing_partial_load(page, search_url, timeout=30000)
        await random_sleep(3, 5)

        if _is_auth_redirect(page.url):
            logger.error("Search still redirects to auth after re-login — URL: %s", page.url)
            raise RuntimeError("LinkedIn search keeps redirecting to login after re-authentication")


async def collect_new_links(page, context, required: int) -> tuple[list[dict], int]:
    new_links: dict[str, dict] = {}
    skipped_count = 0
    max_pages = CONFIG.get("max_search_pages", 10)
    queries = CONFIG.get("search_queries", ["developer"])

    for query in queries:
        if len(new_links) >= required:
            break

        logger.info("Searching query: '%s'", query)

        page_num = 1
        await _goto_search_results(page, context, query, page_num)

        while len(new_links) < required and page_num <= max_pages:
            if _is_auth_redirect(page.url):
                await _goto_search_results(page, context, query, page_num)

            logger.info("Query '%s' — page %s — URL: %s", query, page_num, page.url)
            await full_scroll(page)
            await random_mouse_move(page)
            await random_sleep(2, 3)

            unique_on_page = await _extract_links_with_retries(page, context, query, page_num)
            logger.info("  %s unique links on page %s", len(unique_on_page), page_num)

            existing_keys = await get_existing_success_profile_keys(unique_on_page)
            if existing_keys:
                await touch_profiles_for_keyword(existing_keys, query)

            before = len(new_links)
            for url in unique_on_page:
                try:
                    profile_key = extract_profile_key(url)
                except ValueError:
                    logger.warning("Skipping unsupported LinkedIn URL: %s", url)
                    continue

                if profile_key in existing_keys:
                    skipped_count += 1
                elif profile_key in new_links:
                    if query not in new_links[profile_key]["matched_keywords"]:
                        new_links[profile_key]["matched_keywords"].append(query)
                else:
                    new_links[profile_key] = {
                        "url": url,
                        "profile_key": profile_key,
                        "matched_keywords": [query],
                        "first_found_by": query,
                    }
                    if len(new_links) >= required:
                        break

            added = len(new_links) - before
            logger.info(
                "  +%s new | %s/%s total | %s skipped",
                added,
                len(new_links),
                required,
                skipped_count,
            )

            if len(new_links) >= required:
                break

            if not await _go_to_next_page(page):
                logger.info("Query '%s' exhausted after %s page(s)", query, page_num)
                break

            page_num += 1

    if len(new_links) < required:
        logger.warning(
            "Collected %s/%s new profiles after exhausting all queries",
            len(new_links),
            required,
        )

    return list(new_links.values())[:required], skipped_count


async def scrape_profile(page, url: str) -> dict:
    await page.goto(url, wait_until="domcontentloaded")
    await random_sleep(4, 8)

    await random_mouse_move(page)
    await full_scroll(page)
    await random_sleep(2, 5)
    await random_mouse_move(page)
    await random_sleep(1, 3)

    text: str = await _evaluate_with_navigation_retry(page, "() => document.body.innerText")
    return parse_with_ai(text)


async def _scrape_profile_with_retries(page, url: str, attempts: int = 2) -> dict:
    last_exc = None

    for attempt in range(1, attempts + 1):
        try:
            data = await scrape_profile(page, url)
        except Exception as exc:
            last_exc = exc
            if attempt < attempts and _is_retryable_runtime_error(exc):
                logger.warning(
                    "Transient scrape failure for %s (attempt %s/%s): %s",
                    url,
                    attempt,
                    attempts,
                    exc,
                )
                await random_sleep(3, 5)
                continue
            raise

        if "error" in data and _is_retryable_profile_error(data["error"]) and attempt < attempts:
            logger.warning(
                "Transient profile parse issue for %s (attempt %s/%s): %s",
                url,
                attempt,
                attempts,
                data["error"],
            )
            await random_sleep(3, 5)
            continue

        return data

    if last_exc is not None:
        raise last_exc

    return {"error": "Unknown scrape failure"}


async def run_scraper() -> dict:
    newly_scraped = []

    if not CONFIG["email"] or not CONFIG["password"]:
        raise RuntimeError("Missing LinkedIn credentials. Set LINKEDIN_EMAIL and LINKEDIN_PASSWORD in .env")

    await init_storage()

    async with async_playwright() as playwright:
        browser, context = await launch_browser(playwright)
        page = await context.new_page()

        try:
            await login(page, context)
            new_links, skipped_count = await collect_new_links(
                page,
                context,
                required=CONFIG["max_profiles"],
            )

            if not new_links:
                logger.info("No new profiles to scrape — all already stored")
            else:
                logger.info("Scraping %s new profile(s)...", len(new_links))

            for index, candidate in enumerate(new_links, start=1):
                url = candidate["url"]
                logger.info(
                    "[%s/%s] Scraping: %s | keywords=%s",
                    index,
                    len(new_links),
                    url,
                    ", ".join(candidate["matched_keywords"]),
                )

                try:
                    data = await _scrape_profile_with_retries(page, url)
                    if "error" in data and _is_retryable_profile_error(data["error"]):
                        logger.warning(
                            "Skipping transient failure without saving failed status: %s | %s",
                            url,
                            data["error"],
                        )
                        await random_sleep(CONFIG["min_delay"], CONFIG["max_delay"])
                        continue

                    profile = {**data, "url": url}
                    result = await upsert_profile(
                        profile,
                        matched_keywords=candidate["matched_keywords"],
                        first_found_by=candidate["first_found_by"],
                    )
                    if result["profile"]["status"] == "success":
                        newly_scraped.append(result["profile"])
                    else:
                        logger.warning("Profile saved with failed status: %s", url)
                except Exception as exc:
                    logger.error("Failed to scrape %s: %s", url, exc)
                    if _is_retryable_runtime_error(exc):
                        logger.warning(
                            "Skipping transient runtime failure without saving failed status: %s",
                            url,
                        )
                        await random_sleep(CONFIG["min_delay"], CONFIG["max_delay"])
                        continue

                    await upsert_profile(
                        {"url": url, "error": str(exc)},
                        matched_keywords=candidate["matched_keywords"],
                        first_found_by=candidate["first_found_by"],
                    )

                await random_sleep(CONFIG["min_delay"], CONFIG["max_delay"])

            await save_session(context)

        finally:
            await browser.close()

    return {
        "newly_scraped": len(newly_scraped),
        "skipped_duplicates": skipped_count,
        "data": newly_scraped,
    }
