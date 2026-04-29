"""Fetch rank data from Missevan and Manbo platforms, saving to ranks.json."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from platform_sync import (
    MANBO_HEADERS,
    MANBO_CATALOG_NAME_ALIASES,
    MANBO_CATALOG_NAME_BY_ID,
    MISSEVAN_HEADERS,
    MISSEVAN_CATALOG_NAME_BY_ID,
    MissevanRequester,
    load_json,
    request_manbo_json,
    save_json,
)

# ---------------------------------------------------------------------------
# .env loading
# ---------------------------------------------------------------------------

def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        item = line.strip()
        if not item or item.startswith("#") or "=" not in item:
            continue
        key, value = item.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve().parent
RANKS_PATH = HERE / "ranks.json"
CACHE_WINDOW = timedelta(hours=12)
RANK_HISTORY_RETENTION_DAYS = 90

load_env_file(HERE / ".env")

QUEUE_KEY = "new:dramaIDs"
PLATFORMS = ("missevan", "manbo")
RANK_PARTIAL_KEYS = {
    "missevan": "ranks:partial:missevan",
    "manbo": "ranks:partial:manbo",
}
ONGOING_KEYS = {
    "missevan": "ongoing:missevan",
    "manbo": "ongoing:manbo",
}

# -- Missevan rank definitions (key -> (type, sub_type, display_name)) ------
MISSEVAN_RANKS = {
    "new_daily":          (1, 1, "新品日榜"),
    "new_weekly":         (1, 2, "新品周榜"),
    "popular_weekly":     (2, 2, "人气周榜"),
    "popular_monthly":    (2, 3, "人气月榜"),
    "bestseller_weekly":  (9, 2, "畅销周榜"),
    "bestseller_monthly": (9, 3, "畅销月榜"),
}

# -- Manbo rank definitions (key -> (rankId, display_name, limit, value_field))
MANBO_RANKS = {
    "hot":               (0, "热播榜",       20,   "hotValue"),
    "box_office_total":  (7, "票房榜总榜",    None, "hotValue"),
    "box_office_member": (8, "票房榜会员剧榜", None, "hotValue"),
    "box_office_paid":   (9, "票房榜付费剧榜", None, "hotValue"),
    "diamond_monthly":   (5, "钻石榜月榜",    20,   "diamondValue"),
    "peak":              (4, "巅峰榜",       50,   "hotValue"),
}

MANBO_DANMAKU_PAGE_SIZE = 200
MANBO_DANMAKU_PAGE_CONCURRENCY = 48
MANBO_DANMAKU_DRAMA_CONCURRENCY = 1
MANBO_DANMAKU_REQUEST_RETRIES = 3

# ---------------------------------------------------------------------------
# Upstash helpers (adapted from sync_new_drama_ids.py)
# ---------------------------------------------------------------------------

def upstash_request(command: list[object]) -> object:
    url = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip("/")
    token = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
    if not url or not token:
        raise RuntimeError("Missing UPSTASH_REDIS_REST_URL or UPSTASH_REDIS_REST_TOKEN in environment.")
    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=command,
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise RuntimeError(str(payload["error"]))
    return payload.get("result")


def load_queue() -> dict[str, list[str]]:
    raw = upstash_request(["GET", QUEUE_KEY])
    if raw in (None, ""):
        return {"manbo": [], "missevan": []}
    if isinstance(raw, str):
        data = json.loads(raw)
    elif isinstance(raw, dict):
        data = raw
    else:
        raise RuntimeError(f"Unsupported payload type for {QUEUE_KEY}: {type(raw).__name__}")
    return {
        "manbo": list(dict.fromkeys(str(i) for i in (data.get("manbo") or []))),
        "missevan": list(dict.fromkeys(str(i) for i in (data.get("missevan") or []))),
    }


def save_queue(queue: dict[str, list[str]]) -> None:
    payload = json.dumps(queue, ensure_ascii=False)
    result = upstash_request(["SET", QUEUE_KEY, payload])
    if result != "OK":
        raise RuntimeError(f"Failed to update {QUEUE_KEY}: {result!r}")
    print(f"[ok] updated queue: manbo={len(queue.get('manbo', []))}, missevan={len(queue.get('missevan', []))}")


def append_new_drama_ids_atomic(missevan_ids: list[str], manbo_ids: list[str]) -> None:
    """Atomically merge new drama IDs into Upstash queue."""
    if not missevan_ids and not manbo_ids:
        return
    script = r'''
local raw = redis.call("GET", KEYS[1])
local queue = {missevan = {}, manbo = {}}
if raw and raw ~= false and raw ~= "" then
  queue = cjson.decode(raw)
  queue["missevan"] = queue["missevan"] or {}
  queue["manbo"] = queue["manbo"] or {}
end

local function merge(field, additions_json)
  local seen = {}
  local merged = {}
  for _, value in ipairs(queue[field] or {}) do
    local text = tostring(value)
    if not seen[text] then
      seen[text] = true
      table.insert(merged, text)
    end
  end
  for _, value in ipairs(cjson.decode(additions_json)) do
    local text = tostring(value)
    if not seen[text] then
      seen[text] = true
      table.insert(merged, text)
    end
  end
  queue[field] = merged
end

merge("missevan", ARGV[1])
merge("manbo", ARGV[2])
local payload = cjson.encode(queue)
redis.call("SET", KEYS[1], payload)
return payload
'''
    raw = upstash_request([
        "EVAL",
        script,
        1,
        QUEUE_KEY,
        json.dumps([str(value) for value in missevan_ids], ensure_ascii=False),
        json.dumps([str(value) for value in manbo_ids], ensure_ascii=False),
    ])
    updated = json.loads(raw) if isinstance(raw, str) else (raw or {})
    print(
        f"[ok] updated queue: manbo={len(updated.get('manbo', []))}, "
        f"missevan={len(updated.get('missevan', []))}"
    )


def upload_ranks(store: dict) -> None:
    """Upload ranks store to Upstash under the 'ranks' key."""
    payload = json.dumps(store, ensure_ascii=False)
    result = upstash_request(["SET", "ranks", payload])
    if result != "OK":
        raise RuntimeError(f"Failed to upload ranks: {result!r}")
    print(f"[ok] uploaded ranks to Upstash ({len(payload)} bytes)")


def upload_full_ranks(store: dict) -> None:
    """Upload complete merged ranks under all full-rank keys."""
    payload = json.dumps(store, ensure_ascii=False)
    for key in ("ranks", "ranks:latest"):
        result = upstash_request(["SET", key, payload])
        if result != "OK":
            raise RuntimeError(f"Failed to upload {key}: {result!r}")
    print(f"[ok] uploaded merged ranks to Upstash ({len(payload)} bytes)")


def decode_upstash_json(raw: object, default: object = None) -> object:
    if raw in (None, ""):
        return default
    if isinstance(raw, str):
        return json.loads(raw)
    return raw


def platform_store(value: object) -> dict:
    if isinstance(value, dict):
        value.setdefault("ranks", {})
        value.setdefault("dramas", {})
        return value
    return {"ranks": {}, "dramas": {}}


def build_rank_partial_payload(store: dict, platform: str, *, generated_at: str | None = None) -> dict:
    if platform not in RANK_PARTIAL_KEYS:
        raise ValueError(f"Unsupported platform: {platform}")
    return {
        "version": 1,
        "platform": platform,
        "updated_at": generated_at or now_iso(),
        "data": platform_store(store.get(platform)),
    }


def upload_rank_partials(store: dict, platforms: tuple[str, ...] | list[str]) -> None:
    generated_at = now_iso()
    for platform in platforms:
        key = RANK_PARTIAL_KEYS[platform]
        payload = json.dumps(build_rank_partial_payload(store, platform, generated_at=generated_at), ensure_ascii=False)
        result = upstash_request(["SET", key, payload])
        if result != "OK":
            raise RuntimeError(f"Failed to upload {key}: {result!r}")
        print(f"[ok] uploaded {key} ({len(payload)} bytes)")


def load_rank_partial(platform: str) -> dict | None:
    raw = upstash_request(["GET", RANK_PARTIAL_KEYS[platform]])
    payload = decode_upstash_json(raw)
    if not isinstance(payload, dict):
        return None
    if payload.get("platform") != platform:
        return None
    data = payload.get("data")
    return platform_store(data) if isinstance(data, dict) else None


def load_remote_full_ranks() -> dict | None:
    for key in ("ranks", "ranks:latest"):
        payload = decode_upstash_json(upstash_request(["GET", key]))
        if isinstance(payload, dict):
            return payload
    return None


def merge_rank_partials(
    partials: dict[str, dict | None],
    *,
    fallback_store: dict | None = None,
    local_store: dict | None = None,
    generated_at: str | None = None,
) -> dict:
    merged = init_ranks_store()
    merged["_meta"] = dict((fallback_store or {}).get("_meta") or {})
    merged["_meta"]["updated_at"] = generated_at or now_iso()
    for platform in PLATFORMS:
        source = partials.get(platform)
        if source is None and isinstance(fallback_store, dict):
            source = fallback_store.get(platform)
        if source is None and isinstance(local_store, dict):
            source = local_store.get(platform)
        merged[platform] = platform_store(source)
    return merged


def merge_remote_rank_partials(local_store: dict | None = None) -> dict:
    partials = {platform: load_rank_partial(platform) for platform in PLATFORMS}
    return merge_rank_partials(
        partials,
        fallback_store=load_remote_full_ranks(),
        local_store=local_store,
    )


def merge_and_upload_remote_ranks(local_store: dict | None = None) -> dict:
    merged = merge_remote_rank_partials(local_store)
    upload_full_ranks(merged)
    return merged


def upload_rank_outputs(store: dict, platforms: tuple[str, ...] | list[str]) -> dict:
    upload_rank_partials(store, platforms)
    upload_rank_history(store, platforms=platforms)
    return merge_and_upload_remote_ranks(store)


def latest_rank_history_date() -> str | None:
    try:
        index = load_rank_history_index()
    except Exception as exc:
        print(f"  [upstash] WARN: failed to load ranks:index: {exc}")
        return None
    dates = [str(value) for value in (index.get("dates") or []) if value not in (None, "")]
    return max(dates) if dates else None


def load_rank_metrics(platform: str, history_date: str) -> dict[str, dict]:
    payload = _load_upstash_json(f"ranks:metrics:{history_date}:{platform}")
    if not isinstance(payload, dict):
        return {}
    dramas = payload.get("dramas")
    if not isinstance(dramas, dict):
        return {}
    return {
        str(drama_id): dict(entry)
        for drama_id, entry in dramas.items()
        if isinstance(entry, dict)
    }


def load_remote_platform_store_for_fetch(platform: str) -> dict | None:
    partial = load_rank_partial(platform)
    history_date = latest_rank_history_date()
    if partial is None and history_date is None:
        return None
    metrics = load_rank_metrics(platform, history_date) if history_date else {}
    partial_store = platform_store(partial)
    if partial is None and not metrics:
        return None
    return {
        "ranks": partial_store.get("ranks") or {},
        "dramas": metrics if metrics else (partial_store.get("dramas") or {}),
    }


def load_initial_rank_store() -> dict:
    try:
        remote = init_ranks_store()
        loaded_any = False
        for platform in PLATFORMS:
            platform_store_remote = load_remote_platform_store_for_fetch(platform)
            if platform_store_remote is not None:
                remote[platform] = platform_store(platform_store_remote)
                loaded_any = True
        if loaded_any:
            print("  [upstash] loaded initial ranks store from remote partials/metrics")
            return remote
    except Exception as exc:
        print(f"  [upstash] WARN: failed to load remote initial ranks store: {exc}")

    store = load_json(RANKS_PATH, None)
    if store is not None:
        print(f"  [local] loaded initial ranks store from {RANKS_PATH}")
        return store
    print("  [local] no ranks.json found; initializing empty ranks store")
    return init_ranks_store()


def _coerce_rank_item(item: object, position: int) -> dict:
    row: dict[str, object] = {"position": position}
    if isinstance(item, dict):
        drama_id = item.get("dramaId") or item.get("drama_id")
        drama_ids = item.get("dramaIds") or item.get("drama_ids")
        title = item.get("name") or item.get("title")
        if drama_id is not None:
            row["drama_id"] = str(drama_id)
        if drama_ids:
            row["drama_ids"] = [str(value) for value in drama_ids]
        if title:
            row["title"] = str(title)
            row["series_key"] = str(title)
        for value_name in ("hotValue", "diamondValue", "view_count"):
            if value_name in item:
                row["rank_value"] = item.get(value_name)
                row["rank_value_name"] = value_name
                break
        row["raw"] = item
        return row
    row["drama_id"] = str(item)
    row["raw"] = item
    return row


def _build_rank_list_payload(store: dict, platform: str, history_date: str, generated_at: str) -> dict:
    rank_payload: dict[str, object] = {}
    for rank_key, rank in (store.get(platform, {}).get("ranks") or {}).items():
        if platform == "missevan" and rank_key == "peak":
            continue
        items = rank.get("items") or []
        rank_payload[rank_key] = {
            "name": rank.get("name", rank_key),
            "fetched_at": rank.get("fetched_at"),
            "rankId": rank.get("rankId"),
            "unitName": rank.get("unitName"),
            "items": [
                _coerce_rank_item(item, position)
                for position, item in enumerate(items, 1)
            ],
        }
    return {
        "version": 1,
        "date": history_date,
        "platform": platform,
        "generated_at": generated_at,
        "ranks": rank_payload,
    }


def _build_metric_payload(store: dict, platform: str, history_date: str, generated_at: str) -> dict:
    metric_fields = (
        "name",
        "view_count",
        "danmaku_uid_count",
        "favorite_count",
        "subscription_num",
        "reward_num",
        "reward_total",
        "pay_count",
        "diamond_value",
        "cover",
        "maincvs",
        "catalogName",
        "payStatus",
        "createTime",
        "updated_at",
        "fetched_at",
    )
    dramas: dict[str, dict] = {}
    for drama_id, entry in (store.get(platform, {}).get("dramas") or {}).items():
        if not isinstance(entry, dict):
            continue
        dramas[str(drama_id)] = {
            field: entry.get(field)
            for field in metric_fields
            if field in entry
        }
    return {
        "version": 1,
        "date": history_date,
        "platform": platform,
        "generated_at": generated_at,
        "dramas": dramas,
    }


def build_rank_history_payloads(
    store: dict,
    *,
    platforms: tuple[str, ...] | list[str] = PLATFORMS,
    history_date: str | None = None,
    generated_at: str | None = None,
) -> dict[str, dict]:
    generated = generated_at or now_iso()
    if history_date is None:
        history_date = generated[:10]
    payloads: dict[str, dict] = {}
    for platform in platforms:
        payloads[f"ranks:list:{history_date}:{platform}"] = _build_rank_list_payload(store, platform, history_date, generated)
        payloads[f"ranks:metrics:{history_date}:{platform}"] = _build_metric_payload(store, platform, history_date, generated)
    return payloads


def update_rank_history_index(
    current: dict | None,
    history_date: str,
    *,
    now: str | None = None,
    retention_days: int = RANK_HISTORY_RETENTION_DAYS,
) -> tuple[dict, list[str]]:
    dates = []
    if isinstance(current, dict):
        dates = [str(value) for value in (current.get("dates") or [])]
    previous_dates = set(dates)
    dates.append(history_date)
    dates = sorted(set(dates))[-retention_days:]
    pruned_dates = sorted(previous_dates - set(dates))
    return {
        "version": 1,
        "dates": dates,
        "updated_at": now or now_iso(),
    }, pruned_dates


def load_rank_history_index() -> dict:
    raw = upstash_request(["GET", "ranks:index"])
    if raw in (None, ""):
        return {"version": 1, "dates": [], "updated_at": None}
    if isinstance(raw, str):
        return json.loads(raw)
    if isinstance(raw, dict):
        return raw
    raise RuntimeError(f"Unsupported payload type for ranks:index: {type(raw).__name__}")


def update_rank_history_index_atomic(
    history_date: str,
    *,
    generated_at: str,
    retention_days: int = RANK_HISTORY_RETENTION_DAYS,
) -> list[str]:
    script = r'''
local raw = redis.call("GET", KEYS[1])
local current = {version = 1, dates = {}, updated_at = nil}
if raw and raw ~= false and raw ~= "" then
  current = cjson.decode(raw)
  current["dates"] = current["dates"] or {}
end

local seen = {}
local dates = {}
for _, value in ipairs(current["dates"] or {}) do
  local text = tostring(value)
  if not seen[text] then
    seen[text] = true
    table.insert(dates, text)
  end
end

local history_date = tostring(ARGV[1])
if not seen[history_date] then
  seen[history_date] = true
  table.insert(dates, history_date)
end

table.sort(dates)
local retention_days = tonumber(ARGV[3]) or 90
local pruned = {}
while #dates > retention_days do
  table.insert(pruned, table.remove(dates, 1))
end

local updated = {version = 1, dates = dates, updated_at = tostring(ARGV[2])}
redis.call("SET", KEYS[1], cjson.encode(updated))
return cjson.encode(pruned)
'''
    raw = upstash_request([
        "EVAL",
        script,
        1,
        "ranks:index",
        history_date,
        generated_at,
        str(retention_days),
    ])
    pruned = decode_upstash_json(raw, [])
    return [str(value) for value in pruned] if isinstance(pruned, list) else []


def upload_rank_history(
    store: dict,
    *,
    platforms: tuple[str, ...] | list[str] = PLATFORMS,
    retention_days: int = RANK_HISTORY_RETENTION_DAYS,
) -> None:
    generated_at = now_iso()
    history_date = generated_at[:10]
    payloads = build_rank_history_payloads(
        store,
        platforms=platforms,
        history_date=history_date,
        generated_at=generated_at,
    )
    pruned_dates = update_rank_history_index_atomic(
        history_date,
        generated_at=generated_at,
        retention_days=retention_days,
    )
    for key, value in payloads.items():
        payload = json.dumps(value, ensure_ascii=False)
        result = upstash_request(["SET", key, payload])
        if result != "OK":
            raise RuntimeError(f"Failed to upload {key}: {result!r}")
    for old_date in pruned_dates:
        for platform in PLATFORMS:
            upstash_request(["DEL", f"ranks:list:{old_date}:{platform}"])
            upstash_request(["DEL", f"ranks:metrics:{old_date}:{platform}"])
    print(
        f"[ok] uploaded rank history shards to Upstash "
        f"({len(payloads)} keys, date={history_date}, platforms={','.join(platforms)})"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_stale(fetched_at: str | None, force: bool) -> bool:
    """Return True if the entry should be refreshed."""
    if force or not fetched_at:
        return True
    try:
        fetched = datetime.fromisoformat(fetched_at)
        return datetime.now(timezone.utc) - fetched > CACHE_WINDOW
    except (ValueError, TypeError):
        return True


def select_stale_ids(drama_ids: set[str], existing_dramas: dict, *, force: bool) -> tuple[set[str], int]:
    to_update: set[str] = set()
    skipped = 0
    for did in drama_ids:
        drama_id = str(did)
        existing = existing_dramas.get(drama_id, {}) if isinstance(existing_dramas, dict) else {}
        if is_stale(existing.get("fetched_at") if isinstance(existing, dict) else None, force):
            to_update.add(drama_id)
        else:
            skipped += 1
    return to_update, skipped


def safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def init_ranks_store() -> dict:
    return {
        "_meta": {"updated_at": now_iso()},
        "missevan": {"ranks": {}, "dramas": {}},
        "manbo": {"ranks": {}, "dramas": {}},
    }


def _rank_item_drama_id(item: object) -> str | None:
    if isinstance(item, dict):
        value = item.get("dramaId") or item.get("drama_id") or item.get("id")
    else:
        value = item
    if value in (None, ""):
        return None
    return str(value)


def collect_missevan_danmaku_target_ids(store: dict) -> set[str]:
    """Collect Missevan rank drama IDs whose paid danmaku UID counts should refresh."""
    targets: set[str] = set()
    ranks = store.get("missevan", {}).get("ranks") or {}
    for rank_key, rank in ranks.items():
        if rank_key == "peak":
            continue
        for item in rank.get("items") or []:
            drama_id = _rank_item_drama_id(item)
            if drama_id:
                targets.add(drama_id)
    return targets


def collect_manbo_danmaku_target_ids(store: dict) -> set[str]:
    """Collect Manbo rank drama IDs whose paid danmaku UID counts should refresh."""
    targets: set[str] = set()
    ranks = store.get("manbo", {}).get("ranks") or {}
    for rank_key, rank in ranks.items():
        if rank_key == "peak":
            continue
        for item in rank.get("items") or []:
            drama_id = _rank_item_drama_id(item)
            if drama_id:
                targets.add(drama_id)
    return targets


def extract_ongoing_ids(payload: object) -> set[str]:
    """Extract drama IDs from an ongoing payload."""
    if not isinstance(payload, dict):
        return set()
    records = payload.get("records")
    if not isinstance(records, dict):
        return set()
    ids: set[str] = set()
    for key, record in records.items():
        if not isinstance(record, dict):
            continue
        value = record.get("dramaId")
        if value in (None, ""):
            value = key
        if value not in (None, ""):
            ids.add(str(value))
    return ids


def load_ongoing_drama_ids(platform: str) -> set[str]:
    """Load ongoing drama IDs for one platform from Upstash."""
    key = ONGOING_KEYS[platform]
    return extract_ongoing_ids(_load_upstash_json(key))


def merge_rank_and_ongoing_ids(rank_ids, ongoing_ids) -> set[str]:
    """Merge rank-selected and ongoing drama IDs, coercing values to strings."""
    merged: set[str] = set()
    for value in list(rank_ids or []) + list(ongoing_ids or []):
        if value not in (None, ""):
            merged.add(str(value))
    return merged


def select_manbo_danmaku_backfill_ids(danmaku_ids: set[str], detail_update_ids: set[str]) -> set[str]:
    """Select Manbo danmaku IDs that were not already refreshed with detail updates."""
    return {str(value) for value in (danmaku_ids or set())} - {str(value) for value in (detail_update_ids or set())}


# ---------------------------------------------------------------------------
# Phase 2: Fetch rank lists
# ---------------------------------------------------------------------------

def fetch_missevan_ranks(requester: MissevanRequester, store: dict) -> tuple[set[str], set[str]]:
    """Fetch all Missevan rank lists, updating store. Return (all drama IDs, danmaku-eligible IDs)."""
    all_ids: set[str] = set()
    danmaku_ids: set[str] = set()
    ranks = store["missevan"].setdefault("ranks", {})

    # Standard ranks
    for key, (type_val, sub_type, name) in MISSEVAN_RANKS.items():
        url = f"https://www.missevan.com/rank/details?page_size=30&page=1&type={type_val}&sub_type={sub_type}"
        print(f"  [missevan] fetching rank: {name} ...")
        try:
            data = requester.request_json(url)
        except Exception as exc:
            print(f"  [missevan] WARN: failed to fetch {name}: {exc}")
            continue
        info = data.get("info") or {}
        items_raw = info.get("data") or []
        items = [item["id"] for item in items_raw if "id" in item]
        ranks[key] = {"name": name, "fetched_at": now_iso(), "items": items}
        all_ids.update(str(i) for i in items)
        danmaku_ids.update(str(i) for i in items)
        print(f"  [missevan] {name}: {len(items)} items")

    # Peak rank
    print("  [missevan] fetching rank: 巅峰榜 ...")
    try:
        data = requester.request_json("https://www.missevan.com/x/rank/peak-details")
        outer_data = data.get("data") or {}
        inner_list = outer_data.get("data") or []
        # Each inner_list item has "elements" array
        elements = []
        for group in inner_list:
            elements.extend(group.get("elements") or [])
        # Hardcoded overrides for peak dramaIds
        PEAK_DRAMAID_OVERRIDES: dict[str, list[str]] = {
            "二哈和他的白猫师尊广播剧（翼之声中文配音社团）": ["20741"],
            "娘娘腔（贾诩、苏莫离）": ["19255"],
            "错撩": ["52355"],
        }
        # Build series title -> dramaIds lookup for 猫耳 platform
        series_info = load_json(HERE / "drama-series-info.json", {})
        missevan_series: dict[str, list[str]] = {}
        for entry in series_info.values():
            if entry.get("platform") == "猫耳":
                missevan_series[entry.get("series title", "")] = entry.get("dramaIds", [])
        missevan_series.update(PEAK_DRAMAID_OVERRIDES)
        # Collect names not matched in drama-series-info
        unmatched_names: set[str] = set()
        for el in elements:
            name = el.get("name", "")
            if name and name not in missevan_series:
                unmatched_names.add(name)
        # Fallback: search upstash missevan:info:v1 for unmatched names
        upstash_series: dict[str, list[str]] = {}
        if unmatched_names:
            print(f"  [missevan] 巅峰榜: {len(unmatched_names)} names not in drama-series-info, searching upstash ...")
            missevan_info = _load_upstash_json("missevan:info:v1") or {}
            for drama_id, node in missevan_info.items():
                title = node.get("title", "")
                if title in unmatched_names:
                    upstash_series.setdefault(title, []).append(str(drama_id))
        peak_items = []
        for el in elements:
            name = el.get("name", "")
            cvs = [cv.get("name", "") for cv in (el.get("cvs") or []) if cv.get("name")]
            drama_ids = missevan_series.get(name) or upstash_series.get(name, [])
            peak_items.append({
                "name": name,
                "view_count": el.get("view_count", 0),
                "cover": el.get("cover", ""),
                "cvs": cvs,
                "dramaIds": drama_ids,
            })
        ranks["peak"] = {"name": "巅峰榜", "fetched_at": now_iso(), "items": peak_items}
        print(f"  [missevan] 巅峰榜: {len(peak_items)} items")
    except Exception as exc:
        print(f"  [missevan] WARN: failed to fetch 巅峰榜: {exc}")

    return all_ids, danmaku_ids


def fetch_manbo_ranks(store: dict) -> set[str]:
    """Fetch all Manbo rank lists, updating store. Return set of drama IDs."""
    all_ids: set[str] = set()
    ranks = store["manbo"].setdefault("ranks", {})

    for key, (rank_id, name, limit, value_field) in MANBO_RANKS.items():
        url = f"https://api.kilamanbo.com/api/v530/rank/drama/common/detail?rankId={rank_id}"
        print(f"  [manbo] fetching rank: {name} ...")
        try:
            data = request_manbo_json(url)
        except Exception as exc:
            print(f"  [manbo] WARN: failed to fetch {name}: {exc}")
            continue
        body = data.get("b") or data.get("data") or {}
        drama_list = body.get("radioDramaRespList") or []
        if limit is not None:
            drama_list = drama_list[:limit]
        unit_name = body.get("unitName", "")
        items = []
        for d in drama_list:
            drama_id = str(d.get("radioDramaIdStr") or d.get("radioDramaId", ""))
            val = d.get(value_field, 0)
            if drama_id:
                items.append({"dramaId": drama_id, value_field: val})
                all_ids.add(drama_id)
        ranks[key] = {"name": name, "rankId": rank_id, "unitName": unit_name, "fetched_at": now_iso(), "items": items}
        print(f"  [manbo] {name}: {len(items)} items")

    return all_ids


# ---------------------------------------------------------------------------
# Phase 4: Missevan drama detail collection
# ---------------------------------------------------------------------------

def fetch_missevan_drama_details(
    requester: MissevanRequester,
    drama_ids: set[str],
    store: dict,
    *,
    skip_danmaku: bool,
    danmaku_ids: set[str] | None = None,
) -> None:
    """Fetch detailed info for each Missevan drama ID."""
    dramas = store["missevan"].setdefault("dramas", {})
    total = len(drama_ids)
    for idx, drama_id in enumerate(sorted(drama_ids), 1):
        print(f"  [missevan] ({idx}/{total}) drama {drama_id} ...")
        entry: dict = dramas.get(str(drama_id), {})
        should_skip_dm = skip_danmaku or (danmaku_ids is not None and str(drama_id) not in danmaku_ids)
        try:
            _fetch_one_missevan(requester, str(drama_id), entry, skip_danmaku=should_skip_dm)
        except RuntimeError as exc:
            if "HTTP_418" in str(exc):
                print(f"  [missevan] FATAL: rate limited (418). Saving progress and stopping.")
                dramas[str(drama_id)] = entry
                save_json(RANKS_PATH, store)
                raise
            print(f"  [missevan] ERROR on {drama_id}: {exc}")
        except Exception as exc:
            print(f"  [missevan] ERROR on {drama_id}: {exc}")
        dramas[str(drama_id)] = entry
        save_json(RANKS_PATH, store)


def _fetch_one_missevan(
    requester: MissevanRequester,
    drama_id: str,
    entry: dict,
    *,
    skip_danmaku: bool,
) -> None:
    # Basic drama info
    url = f"https://www.missevan.com/dramaapi/getdrama?drama_id={drama_id}"
    data = requester.request_json(url)
    info = data.get("info") or {}
    drama = info.get("drama") or {}
    episodes_section = info.get("episodes") or {}
    episodes = episodes_section.get("episode") or []

    entry["name"] = drama.get("name", entry.get("name", ""))
    entry["cover"] = drama.get("cover", entry.get("cover", ""))
    entry["view_count"] = drama.get("view_count", 0)

    # Reward detail
    try:
        reward_url = f"https://www.missevan.com/reward/drama-reward-detail?drama_id={drama_id}"
        reward_data = requester.request_json(reward_url)
        reward_info = reward_data.get("info") or {}
        entry["reward_num"] = int(reward_info.get("reward_num") or reward_info.get("data", {}).get("reward_num") or 0)
    except Exception:
        entry.setdefault("reward_num", 0)

    # Reward total (coin sum)
    try:
        rank_url = f"https://www.missevan.com/reward/user-reward-rank?period=3&drama_id={drama_id}"
        rank_data = requester.request_json(rank_url)
        rank_info = rank_data.get("info") or {}
        rank_list = rank_info.get("list") or rank_info.get("data") or []
        if isinstance(rank_list, dict):
            rank_list = rank_list.get("list") or []
        entry["reward_total"] = sum(int(item.get("coin") or 0) for item in rank_list)
    except Exception:
        entry.setdefault("reward_total", 0)

    # updated_at + subscription_num via getdramabysound
    sound_ids = [str(ep.get("sound_id")) for ep in episodes if ep.get("sound_id")]
    if sound_ids:
        try:
            sound_url = f"https://www.missevan.com/dramaapi/getdramabysound?sound_id={sound_ids[0]}"
            sound_data = requester.request_json(sound_url)
            sound_drama = (sound_data.get("info") or {}).get("drama") or {}
            entry["subscription_num"] = sound_drama.get("subscription_num", 0)
            lastupdate = sound_drama.get("lastupdate_time")
            if lastupdate:
                if isinstance(lastupdate, (int, float)):
                    entry["updated_at"] = datetime.fromtimestamp(lastupdate, tz=timezone.utc).isoformat()
                else:
                    entry["updated_at"] = str(lastupdate)
            else:
                entry.setdefault("updated_at", None)
        except Exception:
            entry.setdefault("subscription_num", 0)
            entry.setdefault("updated_at", None)
    else:
        entry["subscription_num"] = 0
        entry["updated_at"] = None

    # Danmaku UID count
    if skip_danmaku:
        entry.setdefault("danmaku_uid_count", None)
    else:
        _fetch_missevan_danmaku(requester, episodes, entry)

    entry["fetched_at"] = now_iso()


def _fetch_missevan_danmaku(requester: MissevanRequester, episodes: list[dict], entry: dict) -> None:
    """Fetch danmaku for paid episodes and count unique UIDs."""
    paid_sounds = []
    for ep in episodes:
        if ep.get("need_pay") in (True, 1, "1") or int(ep.get("price") or 0) > 0:
            sid = ep.get("sound_id")
            if sid:
                paid_sounds.append(str(sid))

    if not paid_sounds:
        entry["danmaku_uid_count"] = 0
        return

    uid_set: set[str] = set()
    for sound_id in paid_sounds:
        try:
            dm_url = f"https://www.missevan.com/sound/getdm?soundid={sound_id}"
            resp = requests.get(dm_url, headers=MISSEVAN_HEADERS, timeout=30)
            resp.raise_for_status()
            _parse_missevan_dm_xml(resp.text, uid_set)
        except Exception as exc:
            print(f"    [danmaku] WARN: failed for sound {sound_id}: {exc}")
        time.sleep(0.35)  # small delay per episode
    entry["danmaku_uid_count"] = len(uid_set)


def _parse_missevan_dm_xml(xml_text: str, uid_set: set[str]) -> None:
    """Parse Missevan danmaku XML, extracting unique UIDs (position 6 in p attr)."""
    for match in re.finditer(r'<d p="([^"]+)"', xml_text):
        parts = match.group(1).split(",")
        if len(parts) > 6 and parts[6]:
            uid_set.add(parts[6])


# ---------------------------------------------------------------------------
# Phase 5: Manbo drama detail collection
# ---------------------------------------------------------------------------

def fetch_manbo_drama_details(
    drama_ids: set[str],
    store: dict,
    *,
    skip_danmaku: bool,
    danmaku_ids: set[str] | None = None,
) -> None:
    """Fetch detailed info for each Manbo drama ID."""
    dramas = store["manbo"].setdefault("dramas", {})
    total = len(drama_ids)
    save_counter = 0
    for idx, drama_id in enumerate(sorted(drama_ids), 1):
        print(f"  [manbo] ({idx}/{total}) drama {drama_id} ...")
        entry: dict = dramas.get(drama_id, {})
        try:
            _fetch_one_manbo(drama_id, entry)
            should_fetch_danmaku = (
                not skip_danmaku
                and (danmaku_ids is None or drama_id in danmaku_ids)
            )
            if should_fetch_danmaku:
                _, uid_count, paid_episode_count = fetch_one_manbo_danmaku_count(drama_id)
                entry["danmaku_uid_count"] = uid_count
                entry["danmaku_paid_episode_count"] = paid_episode_count
        except Exception as exc:
            print(f"  [manbo] ERROR on {drama_id}: {exc}")
        dramas[drama_id] = entry
        save_counter += 1
        if save_counter >= 5:
            save_json(RANKS_PATH, store)
            save_counter = 0
    if save_counter > 0:
        save_json(RANKS_PATH, store)


def _fetch_one_manbo(drama_id: str, entry: dict) -> None:
    url = f"https://api.kilamanbo.com/api/v530/radio/drama/detail?radioDramaId={drama_id}"
    data = request_manbo_json(url)
    body = data.get("b") or data.get("data") or {}

    entry["name"] = body.get("title", entry.get("name", ""))
    entry["cover"] = body.get("coverPic") or body.get("largePic") or body.get("cover", entry.get("cover", ""))
    entry["view_count"] = body.get("watchCount", 0)
    entry["favorite_count"] = body.get("favoriteCount", 0)

    # isVIP: whether the drama is a VIP (member) drama
    vip_free = int(body.get("vipFree") or 0)
    entry["isVIP"] = vip_free == 1

    # pay_count: use memberListenCount for member dramas, payCount otherwise
    pay_count = body.get("payCount", 0)
    member_listen = body.get("memberListenCount", 0)
    if vip_free == 1:
        entry["pay_count"] = member_listen
    else:
        entry["pay_count"] = pay_count

    # diamond_value: from v530 radioDramaRankResp.totalDiamond
    rank_resp = body.get("radioDramaRankResp") or {}
    entry["diamond_value"] = rank_resp.get("totalDiamond", 0)

    # updated_at
    update_time = body.get("updateTime")
    if update_time:
        if isinstance(update_time, (int, float)):
            entry["updated_at"] = datetime.fromtimestamp(update_time / 1000, tz=timezone.utc).isoformat()
        else:
            entry["updated_at"] = str(update_time)
    else:
        entry["updated_at"] = None

    entry["fetched_at"] = now_iso()


def is_paid_manbo_episode(episode: dict) -> bool:
    """Return True when a Manbo episode/set requires payment or membership."""
    return (
        safe_int(episode.get("payType") or episode.get("setPayType")) == 1
        or safe_int(episode.get("vipFree")) == 1
        or safe_int(episode.get("price")) > 0
        or safe_int(episode.get("memberPrice") or episode.get("member_price")) > 0
    )


def _extract_manbo_set_id(episode: dict) -> str | None:
    for key in (
        "radioDramaSetIdStr",
        "radioDramaSetId",
        "dramaSetIdStr",
        "dramaSetId",
        "setId",
        "sound_id",
        "id",
    ):
        value = episode.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _extract_manbo_set_list(payload: dict) -> list[dict]:
    for key in ("setRespList", "radioDramaSetRespList", "dramaSetRespList", "sets"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def fetch_manbo_paid_set_ids(
    drama_id: str,
    *,
    request_json=request_manbo_json,
) -> list[str]:
    """Fetch Manbo drama detail and return paid episode/set IDs."""
    detail_urls = [
        f"https://www.kilamanbo.com/web_manbo/dramaDetail?dramaId={drama_id}",
        f"https://api.kilamanbo.com/api/v530/radio/drama/detail?radioDramaId={drama_id}",
    ]
    for url in detail_urls:
        data = request_json(url)
        payload = data.get("data") or data.get("b") or {}
        episodes = _extract_manbo_set_list(payload)
        paid_ids = []
        for episode in episodes:
            set_id = _extract_manbo_set_id(episode)
            if set_id and is_paid_manbo_episode(episode):
                paid_ids.append(set_id)
        if paid_ids or episodes:
            return list(dict.fromkeys(paid_ids))
    return []


def fetch_manbo_danmaku_users(
    set_id: str,
    *,
    request_json=request_manbo_json,
    page_size: int = MANBO_DANMAKU_PAGE_SIZE,
    page_concurrency: int = MANBO_DANMAKU_PAGE_CONCURRENCY,
    retry_delay: float = 0.5,
) -> set[str]:
    """Fetch Manbo danmaku pages for one episode/set and return unique user eids."""
    def fetch_page(page_no: int) -> tuple[int, set[str]]:
        url = (
            "https://www.kilamanbo.com/web_manbo/getDanmaKuPgList"
            f"?pageSize={page_size}&dramaSetId={set_id}&pageNo={page_no}"
        )
        last_error: Exception | None = None
        for attempt in range(1, MANBO_DANMAKU_REQUEST_RETRIES + 1):
            try:
                data = request_json(url)
                break
            except Exception as exc:
                last_error = exc
                if attempt >= MANBO_DANMAKU_REQUEST_RETRIES:
                    raise
                time.sleep(retry_delay * attempt)
        else:
            raise last_error or RuntimeError("failed to fetch Manbo danmaku page")
        payload = data.get("data") or {}
        entries = payload.get("list") if isinstance(payload, dict) else []
        users = {
            str(item.get("eid"))
            for item in (entries or [])
            if isinstance(item, dict) and item.get("eid") not in (None, "")
        }
        total = safe_int(payload.get("count") if isinstance(payload, dict) else 0, len(users))
        return total, users

    total_count, users = fetch_page(1)
    total_pages = max(1, (total_count + page_size - 1) // page_size)
    if total_pages <= 1:
        return users

    workers = max(1, min(page_concurrency, total_pages - 1))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(fetch_page, page_no) for page_no in range(2, total_pages + 1)]
        for future in as_completed(futures):
            _, page_users = future.result()
            users.update(page_users)
    return users


def _request_manbo_danmaku_page(
    set_id: str,
    page_no: int,
    *,
    request_json=request_manbo_json,
    page_size: int = MANBO_DANMAKU_PAGE_SIZE,
    retry_delay: float = 0.5,
) -> dict:
    url = (
        "https://www.kilamanbo.com/web_manbo/getDanmaKuPgList"
        f"?pageSize={page_size}&dramaSetId={set_id}&pageNo={page_no}"
    )
    last_error: Exception | None = None
    for attempt in range(1, MANBO_DANMAKU_REQUEST_RETRIES + 1):
        try:
            return request_json(url)
        except Exception as exc:
            last_error = exc
            if attempt >= MANBO_DANMAKU_REQUEST_RETRIES:
                raise
            time.sleep(retry_delay * attempt)
    raise last_error or RuntimeError("failed to fetch Manbo danmaku page")


def _extract_manbo_danmaku_page(data: dict, *, page_size: int) -> tuple[int, int, set[str]]:
    payload = data.get("data") or {}
    entries = payload.get("list") if isinstance(payload, dict) else []
    users = {
        str(item.get("eid"))
        for item in (entries or [])
        if isinstance(item, dict) and item.get("eid") not in (None, "")
    }
    total_count = safe_int(payload.get("count") if isinstance(payload, dict) else 0, len(users))
    total_pages = max(1, (total_count + page_size - 1) // page_size)
    return total_count, total_pages, users


def fetch_manbo_paid_danmaku_benchmark(
    drama_id: str,
    *,
    title: str = "",
    request_json=request_manbo_json,
    paid_set_id_loader=fetch_manbo_paid_set_ids,
    page_size: int = MANBO_DANMAKU_PAGE_SIZE,
    page_concurrency: int = MANBO_DANMAKU_PAGE_CONCURRENCY,
    retry_delay: float = 0.5,
) -> dict:
    """Fetch all paid Manbo danmaku pages through one bounded queue and globally dedupe eids."""
    started = time.perf_counter()
    paid_set_ids = paid_set_id_loader(drama_id, request_json=request_json)
    users: set[str] = set()
    failed_pages: list[dict[str, object]] = []
    total_pages_by_set: dict[str, int] = {}
    total_danmaku_by_set: dict[str, int] = {}

    def fetch_and_extract(set_id: str, page_no: int) -> tuple[str, int, int, int, set[str]]:
        data = _request_manbo_danmaku_page(
            set_id,
            page_no,
            request_json=request_json,
            page_size=page_size,
            retry_delay=retry_delay,
        )
        total_count, total_pages, page_users = _extract_manbo_danmaku_page(data, page_size=page_size)
        return set_id, page_no, total_count, total_pages, page_users

    workers = max(1, min(page_concurrency, len(paid_set_ids) or 1))
    first_page_results: list[tuple[str, int, int, int, set[str]]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_page = {
            executor.submit(fetch_and_extract, set_id, 1): (set_id, 1)
            for set_id in paid_set_ids
        }
        for future in as_completed(future_to_page):
            set_id, page_no = future_to_page[future]
            try:
                result = future.result()
            except Exception as exc:
                failed_pages.append({"set_id": set_id, "page_no": page_no, "error": str(exc)})
                continue
            first_page_results.append(result)
            _, _, total_count, total_pages, page_users = result
            total_danmaku_by_set[set_id] = total_count
            total_pages_by_set[set_id] = total_pages
            users.update(page_users)

    remaining_pages = [
        (set_id, page_no)
        for set_id, total_pages in total_pages_by_set.items()
        for page_no in range(2, total_pages + 1)
    ]
    workers = max(1, min(page_concurrency, len(remaining_pages) or 1))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_page = {
            executor.submit(fetch_and_extract, set_id, page_no): (set_id, page_no)
            for set_id, page_no in remaining_pages
        }
        for future in as_completed(future_to_page):
            set_id, page_no = future_to_page[future]
            try:
                _, _, _, _, page_users = future.result()
            except Exception as exc:
                failed_pages.append({"set_id": set_id, "page_no": page_no, "error": str(exc)})
                continue
            users.update(page_users)

    elapsed_seconds = time.perf_counter() - started
    return {
        "drama_id": str(drama_id),
        "title": title,
        "paid_episode_count": len(paid_set_ids),
        "total_pages": sum(total_pages_by_set.values()),
        "unique_user_count": len(users),
        "failed_page_count": len(failed_pages),
        "failed_pages": failed_pages,
        "page_size": page_size,
        "page_concurrency": page_concurrency,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "elapsed": str(timedelta(seconds=elapsed_seconds)),
        "total_danmaku": sum(total_danmaku_by_set.values()),
    }


def fetch_one_manbo_danmaku_count(
    drama_id: str,
    *,
    request_json=request_manbo_json,
) -> tuple[str, int, int]:
    """Return (drama_id, paid_danmaku_uid_count, paid_episode_count) for one Manbo drama."""
    result = fetch_manbo_paid_danmaku_benchmark(
        drama_id,
        request_json=request_json,
        page_concurrency=MANBO_DANMAKU_PAGE_CONCURRENCY,
    )
    failed_pages = result.get("failed_pages") or []
    if failed_pages:
        raise RuntimeError(f"failed Manbo danmaku pages for {drama_id}: {failed_pages!r}")
    return (
        drama_id,
        int(result.get("unique_user_count") or 0),
        int(result.get("paid_episode_count") or 0),
    )


def fetch_manbo_danmaku_details(
    drama_ids: set[str],
    store: dict,
    *,
    force: bool,
) -> None:
    """Refresh paid danmaku UID counts for selected Manbo dramas."""
    manbo_dramas = store["manbo"].setdefault("dramas", {})
    targets = []
    for drama_id in drama_ids:
        entry = manbo_dramas.get(drama_id, {})
        if (
            force
            or entry.get("danmaku_uid_count") is None
            or entry.get("danmaku_paid_episode_count") is None
        ):
            targets.append(drama_id)

    print(f"  [manbo] paid danmaku IDs: total={len(drama_ids)}, update={len(targets)}")
    if not targets:
        return

    save_counter = 0
    workers = max(1, min(MANBO_DANMAKU_DRAMA_CONCURRENCY, len(targets)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_drama = {
            executor.submit(fetch_one_manbo_danmaku_count, drama_id): drama_id
            for drama_id in sorted(targets)
        }
        completed = 0
        for future in as_completed(future_to_drama):
            drama_id = future_to_drama[future]
            completed += 1
            entry = manbo_dramas.get(drama_id, {})
            try:
                _, uid_count, paid_episode_count = future.result()
                entry["danmaku_uid_count"] = uid_count
                entry["danmaku_paid_episode_count"] = paid_episode_count
                entry["fetched_at"] = now_iso()
                print(
                    f"  [manbo] ({completed}/{len(targets)}) drama {drama_id}: "
                    f"{uid_count} paid danmaku IDs from {paid_episode_count} paid episodes"
                )
            except Exception as exc:
                print(f"  [manbo] ({completed}/{len(targets)}) ERROR on {drama_id}: {exc}")
            manbo_dramas[drama_id] = entry
            save_counter += 1
            if save_counter >= 5:
                save_json(RANKS_PATH, store)
                save_counter = 0
    if save_counter > 0:
        save_json(RANKS_PATH, store)


# ---------------------------------------------------------------------------
# Phase 6: Upstash CV lookup
# ---------------------------------------------------------------------------

def catalog_name_from_missevan(node: dict) -> str | None:
    value = node.get("catalog")
    if value in (None, ""):
        return None
    try:
        return MISSEVAN_CATALOG_NAME_BY_ID.get(int(value))
    except (TypeError, ValueError):
        return None


def catalog_name_from_manbo(record: dict) -> str | None:
    name = str(record.get("catalogName") or "").strip()
    if name:
        return MANBO_CATALOG_NAME_ALIASES.get(name, name)
    value = record.get("catalog")
    if value in (None, ""):
        return None
    try:
        return MANBO_CATALOG_NAME_BY_ID.get(int(value))
    except (TypeError, ValueError):
        return None


def pay_status_from_needpay(value: object) -> str | None:
    if value is True:
        return "付费"
    if value is False:
        return "免费"
    return None


def truthy_member_value(value: object) -> bool:
    if value is True:
        return True
    if isinstance(value, (int, float)) and value == 1:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return False


def pay_status_from_metadata(source: dict) -> str | None:
    needpay = source.get("needpay")
    if needpay is False:
        return "免费"
    if needpay is True:
        if truthy_member_value(source.get("is_member")) or truthy_member_value(source.get("vipFree")):
            return "会员"
        return "付费"
    return None


def metadata_create_time(source: dict) -> str | None:
    value = str(source.get("createTime") or "").strip()
    return value or None


def update_metadata_fields(entry: dict, *, catalog_name: str | None, pay_status: str | None, create_time: str | None) -> None:
    if catalog_name is not None:
        entry["catalogName"] = catalog_name
    else:
        entry.setdefault("catalogName", None)
    if pay_status is not None:
        entry["payStatus"] = pay_status
    else:
        entry.setdefault("payStatus", None)
    if create_time is not None:
        entry["createTime"] = create_time
    else:
        entry.setdefault("createTime", None)


def lookup_cvs(store: dict) -> None:
    """Look up main CVs and metadata from Upstash info stores, registering unknown IDs."""
    missevan_dramas = store["missevan"].get("dramas") or {}
    manbo_dramas = store["manbo"].get("dramas") or {}

    if not missevan_dramas and not manbo_dramas:
        print("  [upstash] no dramas to look up CVs for")
        return

    # Load info stores from Upstash
    print("  [upstash] loading missevan:info:v1 ...")
    missevan_info = _load_upstash_json("missevan:info:v1") or {}
    print("  [upstash] loading manbo:info:v1 ...")
    manbo_info_raw = _load_upstash_json("manbo:info:v1") or {}
    manbo_records = manbo_info_raw.get("records") or []
    manbo_by_id: dict[str, dict] = {str(r.get("dramaId", "")): r for r in manbo_records}

    new_missevan: list[str] = []
    new_manbo: list[str] = []

    # Missevan CV lookup
    for drama_id, entry in missevan_dramas.items():
        node = missevan_info.get(str(drama_id))
        if node:
            cvnames = node.get("cvnames") or {}
            maincvs_ids = node.get("maincvs") or []
            entry["maincvs"] = [cvnames.get(str(cid), str(cid)) for cid in maincvs_ids] or None
            update_metadata_fields(
                entry,
                catalog_name=catalog_name_from_missevan(node),
                pay_status=pay_status_from_metadata(node),
                create_time=metadata_create_time(node),
            )
        else:
            entry.setdefault("maincvs", None)
            update_metadata_fields(entry, catalog_name=None, pay_status=None, create_time=None)
            new_missevan.append(str(drama_id))

    # Manbo CV lookup
    for drama_id, entry in manbo_dramas.items():
        record = manbo_by_id.get(drama_id)
        if record:
            entry["maincvs"] = record.get("mainCvNames") or None
            update_metadata_fields(
                entry,
                catalog_name=catalog_name_from_manbo(record),
                pay_status=pay_status_from_metadata(record),
                create_time=metadata_create_time(record),
            )
        else:
            entry.setdefault("maincvs", None)
            update_metadata_fields(entry, catalog_name=None, pay_status=None, create_time=None)
            new_manbo.append(drama_id)

    # Register unknown IDs to queue
    if new_missevan or new_manbo:
        print(f"  [upstash] registering new IDs: missevan={len(new_missevan)}, manbo={len(new_manbo)}")
        try:
            append_new_drama_ids_atomic(new_missevan, new_manbo)
        except Exception as exc:
            print(f"  [upstash] WARN: failed to update queue: {exc}")
    else:
        print("  [upstash] all drama IDs found in info stores")


def _load_upstash_json(key: str) -> dict | list | None:
    try:
        raw = upstash_request(["GET", key])
        if raw in (None, ""):
            return None
        if isinstance(raw, str):
            return json.loads(raw)
        return raw
    except Exception as exc:
        print(f"  [upstash] WARN: failed to load {key}: {exc}")
        return None


# ---------------------------------------------------------------------------
# --only-danmaku mode
# ---------------------------------------------------------------------------

def only_danmaku_mode(store: dict, *, force: bool, do_missevan: bool, do_manbo: bool) -> None:
    """Only update paid danmaku UID counts for rank-selected dramas in ranks.json."""
    requester = MissevanRequester()

    if do_missevan:
        missevan_dramas = store["missevan"].get("dramas") or {}
        ongoing_missevan_ids = load_ongoing_drama_ids("missevan")
        rank_danmaku_ids = collect_missevan_danmaku_target_ids(store)
        danmaku_eligible = merge_rank_and_ongoing_ids(rank_danmaku_ids, ongoing_missevan_ids)
        targets = []
        for drama_id, entry in missevan_dramas.items():
            if drama_id in danmaku_eligible and (force or entry.get("danmaku_uid_count") is None):
                targets.append(drama_id)
        print(
            f"[only-danmaku] missevan: {len(targets)} dramas to update "
            f"(rank={len(rank_danmaku_ids)}, ongoing={len(ongoing_missevan_ids)})"
        )
        for idx, drama_id in enumerate(sorted(targets), 1):
            print(f"  [missevan] ({idx}/{len(targets)}) danmaku for drama {drama_id} ...")
            # Need episodes list from getdrama
            try:
                url = f"https://www.missevan.com/dramaapi/getdrama?drama_id={drama_id}"
                data = requester.request_json(url)
                info = data.get("info") or {}
                episodes = (info.get("episodes") or {}).get("episode") or []
                _fetch_missevan_danmaku(requester, episodes, missevan_dramas[drama_id])
                missevan_dramas[drama_id]["fetched_at"] = now_iso()
            except RuntimeError as exc:
                if "HTTP_418" in str(exc):
                    print(f"  [missevan] FATAL: rate limited. Saving progress and stopping.")
                    save_json(RANKS_PATH, store)
                    raise
                print(f"  [missevan] ERROR: {exc}")
            except Exception as exc:
                print(f"  [missevan] ERROR: {exc}")
            save_json(RANKS_PATH, store)

    if do_manbo:
        manbo_dramas = store["manbo"].get("dramas") or {}
        ongoing_manbo_ids = load_ongoing_drama_ids("manbo")
        rank_danmaku_ids = collect_manbo_danmaku_target_ids(store)
        danmaku_eligible = merge_rank_and_ongoing_ids(rank_danmaku_ids, ongoing_manbo_ids)
        existing_targets = {drama_id for drama_id in danmaku_eligible if drama_id in manbo_dramas}
        print(
            f"[only-danmaku] manbo: {len(existing_targets)} dramas selected "
            f"(rank={len(rank_danmaku_ids)}, ongoing={len(ongoing_manbo_ids)})"
        )
        fetch_manbo_danmaku_details(existing_targets, store, force=force)




# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch rank data from Missevan and Manbo")
    danmaku_group = parser.add_mutually_exclusive_group()
    danmaku_group.add_argument("--skip-danmaku", action="store_true", help="Skip danmaku UID counting")
    danmaku_group.add_argument("--only-danmaku", action="store_true", help="Only update danmaku UID counts for existing dramas")
    parser.add_argument("--force", action="store_true", help="Ignore 12h cache window, force refresh all")
    parser.add_argument("--benchmark-manbo-danmaku", metavar="DRAMA_ID", help="Benchmark one Manbo drama with the unified paid danmaku page queue")
    parser.add_argument("--benchmark-title", default="", help="Title to include in the Manbo benchmark output")
    parser.add_argument("--benchmark-output", default="manbo_huixin_danmaku_benchmark.json", help="Path for Manbo benchmark JSON output")
    parser.add_argument("--benchmark-page-concurrency", type=int, default=MANBO_DANMAKU_PAGE_CONCURRENCY, help="Global page concurrency for Manbo benchmark")
    parser.add_argument("--benchmark-page-size", type=int, default=MANBO_DANMAKU_PAGE_SIZE, help="Page size for Manbo benchmark")
    platform_group = parser.add_mutually_exclusive_group()
    platform_group.add_argument("--missevan-only", action="store_true", help="Only process Missevan")
    platform_group.add_argument("--manbo-only", action="store_true", help="Only process Manbo")
    args = parser.parse_args()

    if args.benchmark_manbo_danmaku:
        print("=== Manbo paid danmaku benchmark ===")
        result = fetch_manbo_paid_danmaku_benchmark(
            str(args.benchmark_manbo_danmaku),
            title=args.benchmark_title,
            page_size=args.benchmark_page_size,
            page_concurrency=args.benchmark_page_concurrency,
        )
        output_path = Path(args.benchmark_output)
        if not output_path.is_absolute():
            output_path = HERE / output_path
        save_json(output_path, result)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        print(f"[ok] benchmark result saved to {output_path}")
        return

    do_missevan = not args.manbo_only
    do_manbo = not args.missevan_only
    active_platforms = tuple(
        platform
        for platform, enabled in (("missevan", do_missevan), ("manbo", do_manbo))
        if enabled
    )

    # Load or init store
    store = load_initial_rank_store()
    store.setdefault("_meta", {})
    store.setdefault("missevan", {"ranks": {}, "dramas": {}})
    store.setdefault("manbo", {"ranks": {}, "dramas": {}})
    store["missevan"].setdefault("ranks", {})
    store["missevan"].setdefault("dramas", {})
    store["manbo"].setdefault("ranks", {})
    store["manbo"].setdefault("dramas", {})

    # --only-danmaku mode: skip everything else
    if args.only_danmaku:
        print("=== Only-danmaku mode ===")
        only_danmaku_mode(store, force=args.force, do_missevan=do_missevan, do_manbo=do_manbo)
        store["_meta"]["updated_at"] = now_iso()
        save_json(RANKS_PATH, store)
        try:
            upload_rank_outputs(store, active_platforms)
        except Exception as exc:
            print(f"  [upstash] WARN: failed to upload rank outputs: {exc}")
        print("=== Done (only-danmaku) ===")
        return

    # Phase 2: Fetch rank lists
    print("=== Phase 2: Fetching rank lists ===")
    requester = MissevanRequester()

    missevan_ids: set[str] = set()
    manbo_ids: set[str] = set()

    missevan_danmaku_ids: set[str] = set()

    if do_missevan:
        missevan_ids, missevan_danmaku_ids = fetch_missevan_ranks(requester, store)
    if do_manbo:
        manbo_ids = fetch_manbo_ranks(store)

    ongoing_missevan_ids: set[str] = set()
    ongoing_manbo_ids: set[str] = set()
    manbo_danmaku_ids: set[str] = set()
    if do_missevan:
        ongoing_missevan_ids = load_ongoing_drama_ids("missevan")
        rank_count = len(missevan_ids)
        missevan_ids = merge_rank_and_ongoing_ids(missevan_ids, ongoing_missevan_ids)
        missevan_danmaku_ids = merge_rank_and_ongoing_ids(missevan_danmaku_ids, ongoing_missevan_ids)
        print(
            f"  [missevan] drama IDs: rank={rank_count}, "
            f"ongoing={len(ongoing_missevan_ids)}, combined={len(missevan_ids)}"
        )
    if do_manbo:
        ongoing_manbo_ids = load_ongoing_drama_ids("manbo")
        rank_count = len(manbo_ids)
        manbo_ids = merge_rank_and_ongoing_ids(manbo_ids, ongoing_manbo_ids)
        manbo_danmaku_ids = merge_rank_and_ongoing_ids(collect_manbo_danmaku_target_ids(store), ongoing_manbo_ids)
        print(
            f"  [manbo] drama IDs: rank={rank_count}, "
            f"ongoing={len(ongoing_manbo_ids)}, combined={len(manbo_ids)}"
        )

    save_json(RANKS_PATH, store)

    # Phase 3: Dedup & cache filter
    print("=== Phase 3: Dedup & cache filtering ===")
    missevan_dramas_existing = store["missevan"].get("dramas") or {}
    manbo_dramas_existing = store["manbo"].get("dramas") or {}

    missevan_to_update, missevan_skipped = select_stale_ids(
        missevan_ids,
        missevan_dramas_existing,
        force=args.force,
    )

    manbo_to_update, manbo_skipped = select_stale_ids(
        manbo_ids,
        manbo_dramas_existing,
        force=args.force,
    )

    print(f"  missevan: total={len(missevan_ids)}, skip={missevan_skipped}, update={len(missevan_to_update)}")
    print(f"  manbo:    total={len(manbo_ids)}, skip={manbo_skipped}, update={len(manbo_to_update)}")

    # Phase 4: Missevan drama details
    if do_missevan and missevan_to_update:
        print(f"=== Phase 4: Missevan drama details ({len(missevan_to_update)}) ===")
        fetch_missevan_drama_details(
            requester, missevan_to_update, store,
            skip_danmaku=args.skip_danmaku,
            danmaku_ids=missevan_danmaku_ids,
        )

    # Phase 5: Manbo drama details
    if do_manbo and manbo_to_update:
        print(f"=== Phase 5: Manbo drama details ({len(manbo_to_update)}) ===")
        fetch_manbo_drama_details(
            manbo_to_update, store,
            skip_danmaku=args.skip_danmaku,
            danmaku_ids=manbo_danmaku_ids,
        )

    if do_manbo and not args.skip_danmaku:
        manbo_backfill_ids = select_manbo_danmaku_backfill_ids(manbo_danmaku_ids, manbo_to_update)
        if manbo_backfill_ids:
            print(f"=== Phase 5b: Manbo paid danmaku backfill ({len(manbo_backfill_ids)}) ===")
            fetch_manbo_danmaku_details(manbo_backfill_ids, store, force=args.force)

    # Phase 6: Upstash CV lookup
    print("=== Phase 6: Upstash CV lookup ===")
    try:
        lookup_cvs(store)
    except Exception as exc:
        print(f"  [upstash] WARN: CV lookup failed: {exc}")

    # Final save
    store["_meta"]["updated_at"] = now_iso()
    save_json(RANKS_PATH, store)

    # Upload to Upstash
    print("=== Uploading ranks to Upstash ===")
    try:
        upload_rank_outputs(store, active_platforms)
    except Exception as exc:
        print(f"  [upstash] WARN: failed to upload rank outputs: {exc}")

    print("=== Done ===")


if __name__ == "__main__":
    main()
