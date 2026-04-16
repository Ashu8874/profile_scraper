"""
services/ai_parser.py — Parses LinkedIn profile text via local Ollama LLM.
"""

import json
import logging
import re
import time
import requests
from app.core.config import OLLAMA_ENDPOINT, OLLAMA_MAX_RETRIES, OLLAMA_MODEL, OLLAMA_TIMEOUT_SEC

logger = logging.getLogger(__name__)

MIN_PAGE_TEXT_LENGTH = 300
BLOCKED_INDICATORS   = [
    "join now", "sign in", "authwall",
    "be the first", "linkedin is better with a free account",
]


def _empty_profile() -> dict:
    return {
        "name": "",
        "headline": "",
        "location": "",
        "experience": [],
        "education": [],
        "skills": [],
        "contact": "",
    }


def is_valid_page_text(text: str) -> bool:
    if len(text.strip()) < MIN_PAGE_TEXT_LENGTH:
        logger.warning("Page text too short — likely auth wall")
        return False
    lower = text.lower()
    for indicator in BLOCKED_INDICATORS:
        if indicator in lower:
            logger.warning(f"Blocked page indicator found: '{indicator}'")
            return False
    return True


def clean_json(text: str) -> str:
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fenced:
        return fenced.group(1).strip()
    brace = re.search(r"\{.*\}", text, re.DOTALL)
    if brace:
        return brace.group(0).strip()
    return text.strip()


def is_empty_profile(data: dict) -> bool:
    return (
        not data.get("name", "").strip()
        and not data.get("headline", "").strip()
        and not data.get("location", "").strip()
        and not data.get("experience")
        and not data.get("skills")
    )


def _normalize_profile(data: dict) -> dict:
    profile = _empty_profile()
    profile["name"] = str(data.get("name", "") or "").strip()
    profile["headline"] = str(data.get("headline", "") or "").strip()
    profile["location"] = str(data.get("location", "") or "").strip()
    profile["contact"] = str(data.get("contact", "") or "").strip()

    experience = data.get("experience") or []
    if isinstance(experience, list):
        profile["experience"] = [
            {
                "title": str(item.get("title", "") or "").strip(),
                "company": str(item.get("company", "") or "").strip(),
                "duration": str(item.get("duration", "") or "").strip(),
            }
            for item in experience
            if isinstance(item, dict)
            and any(str(item.get(key, "") or "").strip() for key in ("title", "company", "duration"))
        ]

    education = data.get("education") or []
    if isinstance(education, list):
        profile["education"] = [
            {
                "institution": str(item.get("institution", "") or "").strip(),
                "degree": str(item.get("degree", "") or "").strip(),
                "dates": str(item.get("dates", "") or "").strip(),
            }
            for item in education
            if isinstance(item, dict)
            and any(str(item.get(key, "") or "").strip() for key in ("institution", "degree", "dates"))
        ]

    skills = data.get("skills") or []
    if isinstance(skills, list):
        seen = set()
        for item in skills:
            value = str(item or "").strip()
            if value and value not in seen:
                seen.add(value)
                profile["skills"].append(value)

    return profile


def _close_json_structure(text: str) -> str:
    s = text.strip()
    if not s:
        return s

    s = re.sub(r",(\s*[}\]])", r"\1", s)
    s = re.sub(r"[\s,]+$", "", s)

    stack: list[str] = []
    in_string = False
    escape = False

    for ch in s:
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == "{":
            stack.append("}")
        elif ch == "[":
            stack.append("]")
        elif ch in "}]":
            if stack and stack[-1] == ch:
                stack.pop()

    if in_string:
        s += '"'

    while s.endswith(":"):
        s = s[:-1].rstrip()
    while s.endswith(","):
        s = s[:-1].rstrip()

    return s + "".join(reversed(stack))


def _try_parse_repaired_json(text: str) -> dict | None:
    candidates = []
    cleaned = clean_json(text)
    if cleaned:
        candidates.append(cleaned)
        repaired = _close_json_structure(cleaned)
        if repaired != cleaned:
            candidates.append(repaired)

    for candidate in candidates:
        try:
            return _normalize_profile(json.loads(candidate))
        except Exception:
            continue

    return None


def _extract_json_string_field(text: str, field: str) -> str:
    match = re.search(rf'"{field}"\s*:\s*"((?:\\.|[^"\\])*)"', text, re.DOTALL)
    if not match:
        return ""
    return bytes(match.group(1), "utf-8").decode("unicode_escape").strip()


def _extract_array_block(text: str, field: str) -> str:
    marker = f'"{field}"'
    start = text.find(marker)
    if start == -1:
        return ""
    start = text.find("[", start)
    if start == -1:
        return ""

    depth = 0
    in_string = False
    escape = False
    chars: list[str] = []

    for ch in text[start:]:
        chars.append(ch)
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                break

    return "".join(chars)


def _salvage_object_array(text: str, field: str, keys: tuple[str, ...]) -> list[dict]:
    block = _extract_array_block(text, field)
    if not block:
        return []

    objects = []
    for raw in re.findall(r"\{.*?\}", block, re.DOTALL):
        item = {}
        for key in keys:
            item[key] = _extract_json_string_field(raw, key)
        if any(item.values()):
            objects.append(item)
    return objects


def _salvage_string_array(text: str, field: str) -> list[str]:
    block = _extract_array_block(text, field)
    if not block:
        return []

    seen = set()
    items = []
    for match in re.findall(r'"((?:\\.|[^"\\])*)"', block):
        value = bytes(match, "utf-8").decode("unicode_escape").strip()
        if value and value not in seen:
            seen.add(value)
            items.append(value)
    return items


def _salvage_profile_from_ai_output(text: str) -> dict:
    profile = _empty_profile()
    for field in ("name", "headline", "location", "contact"):
        profile[field] = _extract_json_string_field(text, field)

    profile["experience"] = _salvage_object_array(text, "experience", ("title", "company", "duration"))
    profile["education"] = _salvage_object_array(text, "education", ("institution", "degree", "dates"))
    profile["skills"] = _salvage_string_array(text, "skills")
    return profile


def _call_ollama(prompt: str) -> str:
    last_error = None

    for attempt in range(1, OLLAMA_MAX_RETRIES + 1):
        try:
            response = requests.post(
                OLLAMA_ENDPOINT,
                json={
                    "model":   OLLAMA_MODEL,
                    "prompt":  prompt,
                    "stream":  False,
                    "options": {"temperature": 0.1, "num_predict": 1024},
                },
                timeout=OLLAMA_TIMEOUT_SEC,
            )
            response.raise_for_status()
            return response.json().get("response", "")
        except requests.RequestException as exc:
            last_error = exc
            if attempt < OLLAMA_MAX_RETRIES:
                logger.warning(
                    "Ollama request attempt %s/%s failed: %s",
                    attempt,
                    OLLAMA_MAX_RETRIES,
                    exc,
                )
                time.sleep(min(attempt * 2, 5))
                continue
            raise

    if last_error is not None:
        raise last_error
    raise RuntimeError("Ollama request failed unexpectedly")


def parse_with_ai(text: str) -> dict:
    """
    Send profile text to Ollama and return structured dict.
    Returns {"error": "..."} on any failure so storage layer skips saving.
    """
    if not is_valid_page_text(text):
        return {"error": "Page text invalid — auth wall or empty page"}

    prompt = f"""Extract structured data from this LinkedIn profile text.

Return ONLY a valid JSON object with no explanation, no markdown, no extra text.

Use this exact structure:
{{
  "name": "",
  "headline": "",
  "location": "",
  "experience": [{{"title": "", "company": "", "duration": ""}}],
  "education": [{{"institution": "", "degree": "", "dates": ""}}],
  "skills": [],
  "contact": ""
}}

LinkedIn Profile Text:
{text[:5000]}
"""

    ai_output = ""
    cleaned   = ""

    try:
        ai_output = _call_ollama(prompt)
        logger.debug(f"Raw AI response: {ai_output[:300]}")

        cleaned = clean_json(ai_output)
        parsed  = _normalize_profile(json.loads(cleaned))

        if is_empty_profile(parsed):
            logger.warning("AI returned empty profile")
            return {"error": "Empty profile — AI could not extract data", "raw": ai_output[:500]}

        return parsed

    except json.JSONDecodeError as e:
        repaired = _try_parse_repaired_json(ai_output)
        if repaired and not is_empty_profile(repaired):
            logger.warning("Recovered malformed AI JSON via repair")
            return repaired

        salvaged = _salvage_profile_from_ai_output(ai_output)
        if not is_empty_profile(salvaged):
            logger.warning("Recovered malformed AI JSON via field salvage")
            return salvaged

        logger.error(f"JSON decode failed: {e} | output: {cleaned[:300]}")
        return {"error": "Invalid JSON from AI", "raw": ai_output[:500]}

    except requests.RequestException as e:
        logger.error(f"Ollama request failed: {e}")
        return {"error": f"AI request failed: {str(e)}"}

    except Exception as e:
        logger.exception("Unexpected error in parse_with_ai")
        return {"error": str(e)}
