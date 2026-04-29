"""Fetch ongoing drama records from Missevan and Manbo and upload to Upstash."""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import requests

from platform_sync import MISSEVAN_HEADERS, MissevanRequester, request_manbo_json

HERE = Path(__file__).resolve().parent
BEIJING_TZ = timezone(timedelta(hours=8))

ONGOING_KEYS = {
    "missevan": "ongoing:missevan",
    "manbo": "ongoing:manbo",
}
MISSEVAN_SUMMERDRAMA_URL = "https://www.missevan.com/dramaapi/summerdrama"
MISSEVAN_TIMELINE_URL = "https://app.missevan.com/drama/timeline"
MISSEVAN_SOUND_PAGE_URL = "https://www.missevan.com/sound/m?order=0&id=17&p={page}"
MISSEVAN_SOUND_DRAMA_URL = "https://www.missevan.com/dramaapi/getdramabysound?sound_id={sound_id}"
MANBO_TIME_DETAIL_URL = (
    "https://api.kilamanbo.com/api/v530/radio/drama/new/time/detail"
    "?date={timestamp}&pageNo=1&pageSize=100&type=105"
)
BLOCKED_UPDATE_TITLE_WORDS = ("福利", "小剧场", "生日")
MISSEVAN_DAILY_VIEW_THRESHOLD = 100
MISSEVAN_DAILY_COMMENT_THRESHOLD = 20


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        item = line.strip()
        if not item or item.startswith("#") or "=" not in item:
            continue
        key, value = item.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


load_env_file(HERE / ".env")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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


def make_record(drama_id: object, update_type: str) -> dict[str, object]:
    return {
        "dramaId": str(drama_id),
        "updateType": update_type,
    }


def merge_records(
    *,
    weekly: list[dict[str, object]] | None = None,
    daily: list[dict[str, object]] | None = None,
) -> dict[str, dict[str, object]]:
    records: dict[str, dict[str, object]] = {}
    for record in weekly or []:
        drama_id = str(record.get("dramaId") or "").strip()
        if drama_id:
            records[drama_id] = dict(record)
    for record in daily or []:
        drama_id = str(record.get("dramaId") or "").strip()
        if drama_id and drama_id not in records:
            records[drama_id] = dict(record)
    return records


def build_payload(platform: str, records: dict[str, dict[str, object]], *, generated_at: str | None = None) -> dict:
    return {
        "version": 1,
        "updatedAt": generated_at or now_iso(),
        "platform": platform,
        "records": records,
    }


def upload_payload(
    platform: str,
    payload: dict,
    *,
    upstash: Callable[[list[object]], object] = upstash_request,
    dry_run: bool = False,
    dry_run_dir: Path = HERE,
) -> Path | None:
    serialized = json.dumps(payload, ensure_ascii=False)
    key = ONGOING_KEYS[platform]
    if dry_run:
        dry_run_dir.mkdir(parents=True, exist_ok=True)
        output_path = dry_run_dir / f"ongoing-{platform}.json"
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(
            f"[dry-run] wrote {output_path}: "
            f"{len(payload.get('records') or {})} records for {key}"
        )
        return output_path
    result = upstash(["SET", key, serialized])
    if result != "OK":
        raise RuntimeError(f"Failed to upload {key}: {result!r}")
    print(f"[ok] uploaded {key}: {len(payload.get('records') or {})} records")
    return None


def build_missevan_timeline_headers() -> dict[str, str] | None:
    raw_headers = os.environ.get("MISSEVAN_TIMELINE_HEADERS_JSON", "").strip()
    if raw_headers:
        data = json.loads(raw_headers)
        if not isinstance(data, dict):
            raise RuntimeError("MISSEVAN_TIMELINE_HEADERS_JSON must be a JSON object.")
        return {str(key): str(value) for key, value in data.items()}

    authorization = os.environ.get("MISSEVAN_TIMELINE_AUTHORIZATION", "").strip()
    cookie = os.environ.get("MISSEVAN_TIMELINE_COOKIE", "").strip()
    x_m_date = os.environ.get("MISSEVAN_TIMELINE_DATE", "").strip()
    x_m_nonce = os.environ.get("MISSEVAN_TIMELINE_NONCE", "").strip()
    if not all((authorization, cookie, x_m_date, x_m_nonce)):
        return None
    return {
        "user-agent": os.environ.get(
            "MISSEVAN_TIMELINE_USER_AGENT",
            "MissEvanApp/6.6.0 (Android;12;Samsung SM-S9210 e1q)",
        ),
        "channel": "missevan",
        "accept": "application/json",
        "cookie": cookie,
        "authorization": authorization,
        "x-m-date": x_m_date,
        "x-m-nonce": x_m_nonce,
    }


def request_missevan_timeline_json() -> dict | None:
    headers = build_missevan_timeline_headers()
    if not headers:
        return None
    response = requests.get(MISSEVAN_TIMELINE_URL, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()
    if data.get("success") is False:
        raise RuntimeError(f"Missevan timeline failed: code={data.get('code')!r}")
    return data


def parse_missevan_timeline_weekly_records(payload: dict) -> list[dict[str, object]]:
    weekly: list[dict[str, object]] = []
    info = payload.get("info") or []
    if not isinstance(info, list):
        return weekly
    for group in info:
        if not isinstance(group, dict):
            continue
        for drama in group.get("dramas") or []:
            if not isinstance(drama, dict):
                continue
            if safe_int(drama.get("pay_type"), -1) != 2:
                continue
            drama_id = drama.get("id")
            if drama_id not in (None, ""):
                weekly.append(make_record(drama_id, "weekly"))
    return weekly


def fetch_missevan_weekly_records(
    requester: MissevanRequester,
    *,
    fetch_timeline: Callable[[], dict | None] = request_missevan_timeline_json,
) -> list[dict[str, object]]:
    try:
        timeline = fetch_timeline()
        if timeline:
            records = parse_missevan_timeline_weekly_records(timeline)
            if records:
                print(f"  [missevan] timeline weekly records={len(records)}")
                return records
            print("  [missevan] WARN: timeline returned no weekly records; falling back to summerdrama")
    except Exception as exc:
        print(f"  [missevan] WARN: timeline fetch failed; falling back to summerdrama: {exc}")

    data = requester.request_json(MISSEVAN_SUMMERDRAMA_URL)
    info = data.get("info") or []
    weekly: list[dict[str, object]] = []
    if not isinstance(info, list):
        return weekly
    for group in info:
        if not isinstance(group, list):
            continue
        for item in group:
            if not isinstance(item, dict):
                continue
            if "pay_type" in item and safe_int(item.get("pay_type"), -1) != 2:
                continue
            drama_id = item.get("id")
            if drama_id not in (None, ""):
                weekly.append(make_record(drama_id, "weekly"))
    return weekly


def parse_missevan_sound_entries(html: str) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    pattern = re.compile(
        r'href="/sound/(?P<sound_id>\d+)"'
        r'(?P<body>.*?vw-frontsound-commentcount\s+floatleft">\s*(?P<comments>[\d,]+)\s*</div>)',
        re.DOTALL,
    )
    for match in pattern.finditer(html or ""):
        body = match.group("body")
        view_match = re.search(r'vw-frontsound-viewcount\s+floatleft">\s*([\d,]+)\s*</div>', body)
        if not view_match:
            continue
        entries.append(
            {
                "soundId": match.group("sound_id"),
                "viewCount": safe_int(view_match.group(1).replace(",", "")),
                "commentCount": safe_int(match.group("comments").replace(",", "")),
            }
        )
    return entries


def fetch_missevan_sound_page(page: int) -> str:
    response = requests.get(MISSEVAN_SOUND_PAGE_URL.format(page=page), headers=MISSEVAN_HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def collect_missevan_daily_sound_ids(
    fetch_html: Callable[[int], str] = fetch_missevan_sound_page,
    *,
    limit: int = 20,
    max_pages: int = 50,
) -> list[str]:
    sound_ids: list[str] = []
    seen: set[str] = set()
    for page in range(1, max_pages + 1):
        html = fetch_html(page)
        entries = parse_missevan_sound_entries(html)
        if not entries:
            break
        for entry in entries:
            sound_id = str(entry.get("soundId") or "")
            if not sound_id or sound_id in seen:
                continue
            if (
                safe_int(entry.get("viewCount")) >= MISSEVAN_DAILY_VIEW_THRESHOLD
                or safe_int(entry.get("commentCount")) >= MISSEVAN_DAILY_COMMENT_THRESHOLD
            ):
                seen.add(sound_id)
                sound_ids.append(sound_id)
                if len(sound_ids) >= limit:
                    return sound_ids
    return sound_ids


def fetch_missevan_daily_drama_ids(
    requester: MissevanRequester,
    sound_ids: list[str],
) -> list[str]:
    drama_ids: list[str] = []
    seen: set[str] = set()
    for sound_id in sound_ids:
        data = requester.request_json(MISSEVAN_SOUND_DRAMA_URL.format(sound_id=sound_id))
        info = data.get("info") or {}
        drama = info.get("drama") if isinstance(info, dict) else {}
        drama_id = (drama or {}).get("id")
        if drama_id in (None, ""):
            continue
        text = str(drama_id)
        if text not in seen:
            seen.add(text)
            drama_ids.append(text)
    return drama_ids


def fetch_missevan_records(requester: MissevanRequester | None = None) -> dict[str, dict[str, object]]:
    active_requester = requester or MissevanRequester()
    weekly = fetch_missevan_weekly_records(active_requester)
    sound_ids = collect_missevan_daily_sound_ids()
    daily_ids = fetch_missevan_daily_drama_ids(active_requester, sound_ids)
    daily = [make_record(drama_id, "daily") for drama_id in daily_ids]
    return merge_records(weekly=weekly, daily=daily)


def previous_7_beijing_midnight_timestamps(*, now: datetime | None = None) -> list[int]:
    current = now or datetime.now(timezone.utc)
    today = current.astimezone(BEIJING_TZ).date()
    timestamps: list[int] = []
    for days_back in range(7, 0, -1):
        midnight = datetime.combine(today - timedelta(days=days_back), datetime.min.time(), tzinfo=BEIJING_TZ)
        timestamps.append(int(midnight.timestamp() * 1000))
    return timestamps


def manbo_labels(item: dict) -> list[str]:
    drama = item.get("radioDramaResp") or {}
    return [
        str(label.get("name") or "").strip()
        for label in drama.get("categoryLabels") or []
        if isinstance(label, dict) and str(label.get("name") or "").strip()
    ]


def is_paid_manbo_ongoing_item(item: dict) -> bool:
    drama = item.get("radioDramaResp") or {}
    price = safe_int(drama.get("price"))
    member_price = safe_int(drama.get("memberPrice"))
    vip_free = safe_int(drama.get("vipFree"))
    return vip_free == 1 or (price > 0 and member_price > 0 and vip_free == 0)


def manbo_update_time_allowed(value: object) -> bool:
    match = re.search(r"(\d{1,2}):(\d{2})", str(value or ""))
    if not match:
        return False
    minutes = int(match.group(1)) * 60 + int(match.group(2))
    return 9 * 60 + 59 <= minutes <= 20 * 60 + 1


def manbo_item_allowed(item: dict) -> bool:
    title = str(item.get("updateSetTitle") or "")
    if any(word in title for word in BLOCKED_UPDATE_TITLE_WORDS):
        return False
    if "全一期" in manbo_labels(item):
        return False
    return is_paid_manbo_ongoing_item(item) and manbo_update_time_allowed(item.get("workUpdateTimeFormat"))


def collect_manbo_records_from_items(items: list[dict]) -> dict[str, dict[str, object]]:
    weekly: list[dict[str, object]] = []
    daily: list[dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict) or not manbo_item_allowed(item):
            continue
        drama = item.get("radioDramaResp") or {}
        drama_id = drama.get("radioDramaIdStr") or item.get("id")
        if drama_id in (None, ""):
            continue
        category = safe_int(drama.get("category"))
        if category == 1:
            weekly.append(make_record(drama_id, "weekly"))
        elif category == 5:
            daily.append(make_record(drama_id, "daily"))
    return merge_records(weekly=weekly, daily=daily)


def extract_manbo_items(payload: dict) -> list[dict]:
    body = payload.get("b") or payload.get("data") or {}
    items = body.get("itemTimeRespList") if isinstance(body, dict) else []
    return [item for item in (items or []) if isinstance(item, dict)]


def fetch_manbo_records() -> dict[str, dict[str, object]]:
    items: list[dict] = []
    for timestamp in previous_7_beijing_midnight_timestamps():
        data = request_manbo_json(MANBO_TIME_DETAIL_URL.format(timestamp=timestamp))
        items.extend(extract_manbo_items(data))
    return collect_manbo_records_from_items(items)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch ongoing drama records from Missevan and Manbo")
    platform_group = parser.add_mutually_exclusive_group()
    platform_group.add_argument("--missevan-only", action="store_true", help="Only process Missevan")
    platform_group.add_argument("--manbo-only", action="store_true", help="Only process Manbo")
    parser.add_argument("--dry-run", action="store_true", help="Print summary without uploading to Upstash")
    args = parser.parse_args()

    do_missevan = not args.manbo_only
    do_manbo = not args.missevan_only

    if do_missevan:
        print("=== Fetching Missevan ongoing dramas ===")
        records = fetch_missevan_records()
        upload_payload("missevan", build_payload("missevan", records), dry_run=args.dry_run)

    if do_manbo:
        print("=== Fetching Manbo ongoing dramas ===")
        records = fetch_manbo_records()
        upload_payload("manbo", build_payload("manbo", records), dry_run=args.dry_run)

    print("=== Done ===")


if __name__ == "__main__":
    main()
