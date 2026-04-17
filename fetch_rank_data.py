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
    MISSEVAN_HEADERS,
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

load_env_file(HERE / ".env")

QUEUE_KEY = "new:dramaIDs"

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


def upload_ranks(store: dict) -> None:
    """Upload ranks store to Upstash under the 'ranks' key."""
    payload = json.dumps(store, ensure_ascii=False)
    result = upstash_request(["SET", "ranks", payload])
    if result != "OK":
        raise RuntimeError(f"Failed to upload ranks: {result!r}")
    print(f"[ok] uploaded ranks to Upstash ({len(payload)} bytes)")


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


def init_ranks_store() -> dict:
    return {
        "_meta": {"updated_at": now_iso()},
        "missevan": {"ranks": {}, "dramas": {}},
        "manbo": {"ranks": {}, "dramas": {}},
    }


# ---------------------------------------------------------------------------
# Phase 2: Fetch rank lists
# ---------------------------------------------------------------------------

def fetch_missevan_ranks(requester: MissevanRequester, store: dict) -> tuple[set[str], set[str]]:
    """Fetch all Missevan rank lists, updating store. Return (all drama IDs, danmaku-eligible IDs)."""
    all_ids: set[str] = set()
    danmaku_ids: set[str] = set()
    ranks = store["missevan"].setdefault("ranks", {})

    # Ranks whose dramas should have danmaku collected
    DANMAKU_RANKS = {"new_daily", "new_weekly"}

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
        if key in DANMAKU_RANKS:
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


# ---------------------------------------------------------------------------
# Phase 6: Upstash CV lookup
# ---------------------------------------------------------------------------

def lookup_cvs(store: dict) -> None:
    """Look up main CVs from Upstash info stores and register unknown IDs."""
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
        else:
            entry.setdefault("maincvs", None)
            new_missevan.append(str(drama_id))

    # Manbo CV lookup
    for drama_id, entry in manbo_dramas.items():
        record = manbo_by_id.get(drama_id)
        if record:
            entry["maincvs"] = record.get("mainCvNames") or None
        else:
            entry.setdefault("maincvs", None)
            new_manbo.append(drama_id)

    # Register unknown IDs to queue
    if new_missevan or new_manbo:
        print(f"  [upstash] registering new IDs: missevan={len(new_missevan)}, manbo={len(new_manbo)}")
        try:
            queue = load_queue()
            existing_m = set(queue.get("missevan") or [])
            existing_b = set(queue.get("manbo") or [])
            existing_m.update(new_missevan)
            existing_b.update(new_manbo)
            queue["missevan"] = list(existing_m)
            queue["manbo"] = list(existing_b)
            save_queue(queue)
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

def only_danmaku_mode(store: dict, *, force: bool, do_missevan: bool) -> None:
    """Only update danmaku_uid_count for Missevan dramas (新品日榜+新品周榜) in ranks.json."""
    requester = MissevanRequester()

    if do_missevan:
        missevan_dramas = store["missevan"].get("dramas") or {}
        # Only process dramas from 新品日榜 and 新品周榜
        missevan_ranks = store["missevan"].get("ranks") or {}
        danmaku_eligible = set()
        for rk in ("new_daily", "new_weekly"):
            danmaku_eligible.update(str(i) for i in (missevan_ranks.get(rk, {}).get("items") or []))
        targets = []
        for drama_id, entry in missevan_dramas.items():
            if drama_id in danmaku_eligible and (force or entry.get("danmaku_uid_count") is None):
                targets.append(drama_id)
        print(f"[only-danmaku] missevan: {len(targets)} dramas to update (from 新品日榜+新品周榜)")
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




# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch rank data from Missevan and Manbo")
    danmaku_group = parser.add_mutually_exclusive_group()
    danmaku_group.add_argument("--skip-danmaku", action="store_true", help="Skip danmaku UID counting")
    danmaku_group.add_argument("--only-danmaku", action="store_true", help="Only update danmaku UID counts for existing dramas")
    parser.add_argument("--force", action="store_true", help="Ignore 12h cache window, force refresh all")
    platform_group = parser.add_mutually_exclusive_group()
    platform_group.add_argument("--missevan-only", action="store_true", help="Only process Missevan")
    platform_group.add_argument("--manbo-only", action="store_true", help="Only process Manbo")
    args = parser.parse_args()

    do_missevan = not args.manbo_only
    do_manbo = not args.missevan_only

    # Load or init store
    store = load_json(RANKS_PATH, None)
    if store is None:
        store = init_ranks_store()
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
        only_danmaku_mode(store, force=args.force, do_missevan=do_missevan)
        store["_meta"]["updated_at"] = now_iso()
        save_json(RANKS_PATH, store)
        try:
            upload_ranks(store)
        except Exception as exc:
            print(f"  [upstash] WARN: failed to upload ranks: {exc}")
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

    save_json(RANKS_PATH, store)

    # Phase 3: Dedup & cache filter
    print("=== Phase 3: Dedup & cache filtering ===")
    missevan_dramas_existing = store["missevan"].get("dramas") or {}
    manbo_dramas_existing = store["manbo"].get("dramas") or {}

    missevan_to_update = set()
    missevan_skipped = 0
    for did in missevan_ids:
        existing = missevan_dramas_existing.get(str(did), {})
        if is_stale(existing.get("fetched_at"), args.force):
            missevan_to_update.add(str(did))
        else:
            missevan_skipped += 1

    manbo_to_update = set()
    manbo_skipped = 0
    for did in manbo_ids:
        existing = manbo_dramas_existing.get(did, {})
        if is_stale(existing.get("fetched_at"), args.force):
            manbo_to_update.add(did)
        else:
            manbo_skipped += 1

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
        )

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
        upload_ranks(store)
    except Exception as exc:
        print(f"  [upstash] WARN: failed to upload ranks: {exc}")

    print("=== Done ===")


if __name__ == "__main__":
    main()
