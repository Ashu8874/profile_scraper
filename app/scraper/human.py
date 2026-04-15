"""
scraper/human.py — Human behaviour simulation: delays, scrolling, mouse, typing.
"""

import asyncio
import random


async def random_sleep(min_sec: float = 2.0, max_sec: float = 6.0):
    await asyncio.sleep(random.uniform(min_sec, max_sec))


async def full_scroll(page):
    """Scroll page like a human — variable speed with occasional scroll-back."""
    for _ in range(random.randint(4, 8)):
        await page.mouse.wheel(0, random.randint(400, 900))
        await asyncio.sleep(random.uniform(0.5, 2.0))
        if random.random() < 0.25:
            await page.mouse.wheel(0, -random.randint(100, 300))
            await asyncio.sleep(random.uniform(0.3, 1.0))


async def random_mouse_move(page):
    """Move mouse to random viewport positions."""
    vp    = page.viewport_size or {"width": 1280, "height": 800}
    for _ in range(random.randint(2, 5)):
        await page.mouse.move(
            random.randint(100, vp["width"] - 100),
            random.randint(100, vp["height"] - 100),
        )
        await asyncio.sleep(random.uniform(0.1, 0.4))


async def human_type(page, selector: str, text: str):
    """Type character-by-character with random keystroke delays."""
    await page.click(selector)
    await asyncio.sleep(random.uniform(0.3, 0.7))
    for char in text:
        await page.type(selector, char, delay=random.randint(60, 180))
        if random.random() < 0.1:
            await asyncio.sleep(random.uniform(0.2, 0.5))
