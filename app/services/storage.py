"""
services/storage.py — Production MongoDB storage for scraped LinkedIn profiles.
"""

import asyncio
import inspect
import logging
import re
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import unquote, urlsplit, urlunsplit

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient, IndexModel

from app.core.config import (
    MONGODB_COLLECTION,
    MONGODB_CONNECT_TIMEOUT_MS,
    MONGODB_DATABASE,
    MONGODB_MAX_POOL_SIZE,
    MONGODB_MIN_POOL_SIZE,
    MONGODB_SERVER_SELECTION_TIMEOUT_MS,
    MONGODB_URI,
)

logger = logging.getLogger(__name__)

_client: AsyncMongoClient | None = None
_collection = None
_init_lock = asyncio.Lock()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_profile_url(url: str) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        raise ValueError("Profile URL is required")

    parsed = urlsplit(cleaned)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower() or "www.linkedin.com"
    path = re.sub(r"/{2,}", "/", parsed.path or "/").rstrip("/")

    if not path.startswith("/"):
        path = "/" + path
    if not path:
        path = "/"

    return urlunsplit((scheme, netloc, f"{path}/", "", ""))


def extract_profile_key(url: str) -> str:
    normalized = normalize_profile_url(url)
    parts = [part for part in urlsplit(normalized).path.split("/") if part]
    if len(parts) >= 2 and parts[0].lower() == "in":
        return unquote(parts[1]).strip().lower()
    raise ValueError(f"Unsupported LinkedIn profile URL: {url}")


def _clean_keywords(keywords: Iterable[str]) -> list[str]:
    cleaned = []
    seen = set()
    for keyword in keywords:
        value = (keyword or "").strip()
        if value and value not in seen:
            seen.add(value)
            cleaned.append(value)
    return cleaned


def _is_meaningful_profile(profile: dict) -> bool:
    if (profile.get("name") or "").strip():
        return True
    if (profile.get("headline") or "").strip():
        return True
    if (profile.get("location") or "").strip():
        return True
    if (profile.get("contact") or "").strip():
        return True
    if profile.get("experience"):
        return True
    if profile.get("education"):
        return True
    if profile.get("skills"):
        return True
    return False


async def init_storage():
    global _client, _collection

    if _collection is not None:
        return

    async with _init_lock:
        if _collection is not None:
            return

        client = AsyncMongoClient(
            MONGODB_URI,
            maxPoolSize=MONGODB_MAX_POOL_SIZE,
            minPoolSize=MONGODB_MIN_POOL_SIZE,
            serverSelectionTimeoutMS=MONGODB_SERVER_SELECTION_TIMEOUT_MS,
            connectTimeoutMS=MONGODB_CONNECT_TIMEOUT_MS,
        )
        await client.admin.command("ping")

        collection = client[MONGODB_DATABASE][MONGODB_COLLECTION]
        await collection.create_indexes([
            IndexModel([("profile_key", ASCENDING)], unique=True, name="uniq_profile_key"),
            IndexModel([("matched_keywords", ASCENDING)], name="matched_keywords_idx"),
            IndexModel([("first_found_by", ASCENDING)], name="first_found_by_idx"),
            IndexModel([("status", ASCENDING), ("updated_at", DESCENDING)], name="status_updated_idx"),
            IndexModel([("last_seen_at", DESCENDING)], name="last_seen_at_idx"),
        ])

        _client = client
        _collection = collection
        logger.info(
            "MongoDB storage ready: %s.%s",
            MONGODB_DATABASE,
            MONGODB_COLLECTION,
        )


async def close_storage():
    global _client, _collection

    client = _client
    _client = None
    _collection = None

    if client is not None:
        close_result = client.close()
        if inspect.isawaitable(close_result):
            await close_result
        logger.info("MongoDB connection closed")


async def _get_collection():
    if _collection is None:
        await init_storage()
    return _collection


async def get_existing_success_profile_keys(urls: Iterable[str]) -> set[str]:
    collection = await _get_collection()
    profile_keys = set()
    for url in urls:
        if not url:
            continue
        try:
            profile_keys.add(extract_profile_key(url))
        except ValueError:
            logger.warning("Skipping invalid profile URL during dedupe check: %s", url)

    if not profile_keys:
        return set()

    keys = sorted(profile_keys)
    cursor = collection.find(
        {"profile_key": {"$in": keys}, "status": "success"},
        {"profile_key": 1},
    )
    return {doc["profile_key"] async for doc in cursor}


async def touch_profiles_for_keyword(profile_keys: Iterable[str], keyword: str) -> int:
    collection = await _get_collection()
    keys = sorted({key for key in profile_keys if key})
    keyword = (keyword or "").strip()

    if not keys or not keyword:
        return 0

    result = await collection.update_many(
        {"profile_key": {"$in": keys}, "status": "success"},
        {
            "$addToSet": {"matched_keywords": keyword},
            "$set": {
                "last_found_by": keyword,
                "last_seen_at": _utcnow(),
            },
        },
    )
    return result.modified_count


async def upsert_profile(profile: dict, matched_keywords: Iterable[str], first_found_by: str) -> dict:
    collection = await _get_collection()

    url = normalize_profile_url(profile.get("url", ""))
    profile_key = extract_profile_key(url)
    keywords = _clean_keywords(matched_keywords)
    first_keyword = (first_found_by or "").strip() or (keywords[0] if keywords else "")
    last_keyword = keywords[-1] if keywords else first_keyword
    now = _utcnow()
    is_failed_profile = bool((profile.get("error") or "").strip())

    if not is_failed_profile and not _is_meaningful_profile(profile):
        raise ValueError("Profile has no meaningful data to store")

    if is_failed_profile:
        set_fields = {
            "profile_key": profile_key,
            "url": url,
            "status": "failed",
            "error": (profile.get("error") or "").strip(),
            "raw": (profile.get("raw") or "").strip(),
            "last_found_by": last_keyword,
            "last_seen_at": now,
            "scraped_at": now,
            "updated_at": now,
            "source": "linkedin",
        }

        update = {
            "$set": set_fields,
            "$setOnInsert": {
                "created_at": now,
                "first_seen_at": now,
                "first_found_by": first_keyword,
            },
        }

        if keywords:
            update["$addToSet"] = {"matched_keywords": {"$each": keywords}}

        result = await collection.update_one(
            {"profile_key": profile_key},
            update,
            upsert=True,
        )

        stored_profile = {
            "url": url,
            "profile_key": profile_key,
            "matched_keywords": keywords,
            "first_found_by": first_keyword,
            "last_found_by": last_keyword,
            "status": "failed",
            "error": set_fields["error"],
            "raw": set_fields["raw"],
            "scraped_at": now,
            "updated_at": now,
        }

        return {
            "created": result.upserted_id is not None,
            "profile": stored_profile,
        }

    set_fields = {
        "profile_key": profile_key,
        "url": url,
        "name": profile.get("name", "").strip(),
        "headline": profile.get("headline", "").strip(),
        "location": profile.get("location", "").strip(),
        "experience": profile.get("experience") or [],
        "education": profile.get("education") or [],
        "skills": profile.get("skills") or [],
        "contact": profile.get("contact", "").strip(),
        "status": "success",
        "last_found_by": last_keyword,
        "last_seen_at": now,
        "scraped_at": now,
        "updated_at": now,
        "source": "linkedin",
    }

    update = {
        "$set": set_fields,
        "$setOnInsert": {
            "created_at": now,
            "first_seen_at": now,
            "first_found_by": first_keyword,
        },
    }

    if keywords:
        update["$addToSet"] = {"matched_keywords": {"$each": keywords}}

    update["$unset"] = {"error": "", "raw": ""}

    result = await collection.update_one(
        {"profile_key": profile_key},
        update,
        upsert=True,
    )

    stored_profile = {
        **profile,
        "url": url,
        "profile_key": profile_key,
        "matched_keywords": keywords,
        "first_found_by": first_keyword,
        "last_found_by": last_keyword,
        "status": "success",
        "scraped_at": now,
        "updated_at": now,
    }

    return {
        "created": result.upserted_id is not None,
        "profile": stored_profile,
    }


async def append_profile(profile: dict) -> bool:
    matched_keywords = profile.get("matched_keywords") or []
    first_found_by = profile.get("first_found_by") or ""
    result = await upsert_profile(profile, matched_keywords, first_found_by)
    return result["created"]


async def get_scraped_urls() -> set[str]:
    collection = await _get_collection()
    cursor = collection.find({"status": "success"}, {"url": 1})
    urls = set()
    async for doc in cursor:
        if doc.get("url"):
            urls.add(doc["url"])
    return urls


async def get_stats() -> dict:
    collection = await _get_collection()
    total = await collection.count_documents({})
    successful = await collection.count_documents({"status": "success"})
    failed = await collection.count_documents({"status": "failed"})
    return {
        "total": total,
        "successful": successful,
        "failed": failed,
    }


async def purge_non_success_profiles() -> int:
    collection = await _get_collection()
    result = await collection.delete_many({"status": {"$ne": "success"}})
    return result.deleted_count
