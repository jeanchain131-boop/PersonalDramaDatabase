from __future__ import annotations

import json
import random
import re
import time
import unicodedata
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import requests


ROOT = Path(__file__).resolve().parent
SQLITE_PATH = ROOT / "DramasByCV.sqlite"
MERGED_PATH = ROOT / "DramasByCV_merged.xlsx"
MISSEVAN_INFO_PATH = ROOT / "missevan-drama-info.json"
MANBO_INFO_PATH = ROOT / "manbo-drama-info.json"
MISSEVAN_COUNTS_PATH = ROOT / "missevan-watch-counts.json"
MANBO_COUNTS_PATH = ROOT / "manbo-watch-counts.json"
COMBINED_CVID_MAP_PATH = ROOT / "missevan&manbo-cvid-map.json"
SERIES_INFO_PATH = ROOT / "drama-series-info.json"

MISSEVAN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Referer": "https://www.missevan.com/",
    "Accept": "application/json, text/plain, */*",
}
MANBO_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
}
BJ_TZ = timezone(timedelta(hours=8))

GENRE_BY_TYPE = {3: "全年龄", 4: "纯爱", 6: "言情"}
MISSEVAN_CATALOG_NAME_BY_ID = {89: "广播剧", 90: "广播剧", 93: "有声剧", 96: "有声漫"}
MANBO_CATALOG_NAME_BY_ID = {1: "广播剧", 5: "有声剧"}
MANBO_CATALOG_NAME_ALIASES = {"有声书": "有声剧"}
CATALOG_SUFFIX_BY_NAME = {
    "广播剧": "广播剧",
    "有声剧": "有声剧",
    "有声漫": "有声漫",
}
EPISODE_NUMBER_PATTERN = r"[0-9零一二两三四五六七八九十百千万壹贰叁肆伍陆柒捌玖拾佰仟]+"
EPISODE_PATTERNS = [
    re.compile(rf"第\s*{EPISODE_NUMBER_PATTERN}\s*[集话期章节卷杯单篇回]", re.I | re.U),
    re.compile(r"(?:^|\s)ep\.?\s*[0-9]+", re.I | re.U),
    re.compile(r"(?:^|\s)e[0-9]{{1,3}}(?:\D|$)", re.I | re.U),
    re.compile(r"episode\s*[0-9]+", re.I | re.U),
    re.compile(r"s\s*[0-9]{1,2}\s*e\s*0*1(?:\D|$)", re.I | re.U),
    re.compile(r"^0*1\b", re.I | re.U),
    re.compile(r"^0*1[·.、\s\-_:：](?:\D|$)", re.I | re.U),
    re.compile(r"0*1\s*集(?:\D|$)", re.I | re.U),
    re.compile(r"(?:^|[\s:：\-_.·])0*1(?:\D|$)", re.I | re.U),
    re.compile(r"[A-Za-z\u4e00-\u9fff》」』】）)]\s*0*1(?:\D|$)", re.I | re.U),
    re.compile(rf"[（(＜<【\[]\s*(?:1|0*1|一)\s*[）)＞>】\]]", re.I | re.U),
    re.compile(r"(?:上集)(?:\D|$)", re.I | re.U),
]
NARRATOR_ROLES = {"旁白", "报幕"}
NON_MAIN_EPISODE_KEYWORDS = (
    "预告",
    "pv",
    "番外",
    "花絮",
    "采访",
    "ed",
    "op",
    "ft",
    "翻唱",
)
THEME_SONG_KEYWORDS = ("主题曲",)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _missevan_node_sort_key(node: dict) -> tuple[int, str]:
    drama_id = normalize(node.get("dramaId"))
    try:
        return int(drama_id), drama_id
    except ValueError:
        return 10**18, drama_id


def _missevan_node_signature(node: dict, *, ignore_create_time: bool = False) -> str:
    payload = deepcopy(node)
    if ignore_create_time:
        payload.pop("createTime", None)
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _missevan_node_score(node: dict) -> int:
    def score_value(value) -> int:
        if isinstance(value, dict):
            return sum(score_value(item) for item in value.values())
        if isinstance(value, list):
            return len(value) + sum(score_value(item) for item in value)
        return 0 if value in (None, "") else 1

    return score_value(node)


def _earliest_month(values: list[object]) -> str:
    months = [normalize(value) for value in values if normalize(value)]
    return min(months) if months else ""


def iter_missevan_nodes(store: dict):
    for outer_key in sorted(store, key=lambda value: normalize(value)):
        value = store.get(outer_key) or {}
        if not isinstance(value, dict):
            continue
        if "dramaId" in value:
            yield str(outer_key), "season1", value
            continue
        for season_key in sorted(value, key=lambda item: normalize(item)):
            node = value.get(season_key) or {}
            if isinstance(node, dict):
                yield str(outer_key), str(season_key), node


def remove_missevan_node(store: dict, outer_key: str, season_key: str) -> None:
    value = store.get(outer_key) or {}
    if isinstance(value, dict) and "dramaId" in value:
        store.pop(outer_key, None)
        return

    if isinstance(value, dict):
        value.pop(season_key, None)
        if value:
            store[outer_key] = value
        else:
            store.pop(outer_key, None)


def flatten_missevan_store(store: dict) -> tuple[dict[str, dict], list[str]]:
    indexed: dict[str, dict] = {}
    conflicts: list[str] = []
    conflict_seen: set[str] = set()

    def remember_conflict(drama_id: str) -> None:
        if drama_id not in conflict_seen:
            conflict_seen.add(drama_id)
            conflicts.append(drama_id)

    for _outer_key, _season_key, node in iter_missevan_nodes(store or {}):
        drama_id = normalize(node.get("dramaId"))
        if not drama_id:
            continue
        candidate = deepcopy(node)
        existing = indexed.get(drama_id)
        if existing is None:
            indexed[drama_id] = candidate
            continue
        if _missevan_node_signature(existing) == _missevan_node_signature(candidate):
            continue
        if _missevan_node_signature(existing, ignore_create_time=True) == _missevan_node_signature(candidate, ignore_create_time=True):
            create_time = _earliest_month([existing.get("createTime"), candidate.get("createTime")])
            if create_time:
                existing["createTime"] = create_time
            continue
        remember_conflict(drama_id)
        existing_score = _missevan_node_score(existing)
        candidate_score = _missevan_node_score(candidate)
        if candidate_score > existing_score:
            indexed[drama_id] = candidate

    return {
        drama_id: indexed[drama_id]
        for drama_id in sorted(indexed, key=lambda key: _missevan_node_sort_key(indexed[key]))
    }, conflicts


def finalize_missevan_store(store: dict) -> tuple[dict[str, dict], list[str]]:
    records: list[dict] = []
    for _outer_key, _season_key, node in iter_missevan_nodes(store or {}):
        if not isinstance(node, dict):
            continue
        catalog = node.get("catalog")
        records.append(
            {
                "title": normalize(node.get("title")),
                "catalog": catalog,
                "catalogName": MISSEVAN_CATALOG_NAME_BY_ID.get(int(catalog)) if catalog not in (None, "") else "",
                "node": node,
            }
        )
    finalize_series_titles(records, title_key="title", catalog_key="catalog", catalog_name_key="catalogName", output_key="seriesTitle")
    for item in records:
        item["node"]["seriesTitle"] = item["seriesTitle"]
    return flatten_missevan_store({str(idx): {"season1": item["node"]} for idx, item in enumerate(records)})


def save_missevan_store(path: Path, store: dict) -> list[str]:
    flat_store, conflicts = finalize_missevan_store(store)
    save_json(path, flat_store)
    return conflicts


def load_cache(path: Path) -> dict:
    data = load_json(path, {"_meta": {"updated_at": None}, "counts": {}})
    if not isinstance(data, dict):
        data = {"_meta": {"updated_at": None}, "counts": {}}
    data.setdefault("_meta", {"updated_at": None})
    data.setdefault("counts", {})
    return data


def save_cache(path: Path, cache: dict) -> None:
    cache.setdefault("_meta", {})
    cache["_meta"]["updated_at"] = utc_now()
    save_json(path, cache)


def normalize(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split()).strip()


def normalize_match(value: object) -> str:
    return re.sub(r"\s+", "", normalize(value)).casefold()


def normalize_text_for_match(value: object) -> str:
    text = unicodedata.normalize("NFKC", normalize(value))
    return re.sub(r"\s+", " ", text).strip()


def split_csv(value: object) -> list[str]:
    text = normalize(value)
    if not text:
        return []
    out: list[str] = []
    for part in text.split(","):
        item = part.strip()
        if item and item not in out:
            out.append(item)
    return out


def safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def to_beijing_month(ts: int | float | None, *, milliseconds: bool = False) -> str:
    if ts in (None, "", 0):
        return ""
    value = float(ts)
    if milliseconds:
        value /= 1000.0
    dt = datetime.fromtimestamp(value, tz=timezone.utc).astimezone(BJ_TZ)
    return dt.strftime("%Y.%m")


def clean_role_name(value: object) -> str:
    text = normalize_text_for_match(value)
    text = re.sub(r"^(?:饰[:：]\s*|cv[:：]\s*)", "", text, flags=re.I)
    parts = re.split(r"\s*[\/／|｜]+\s*", text)
    kept: list[str] = []
    for part in parts:
        token = normalize(part)
        token = re.sub(r"\s+", "", token)
        if not token:
            continue
        if token in NARRATOR_ROLES:
            continue
        kept.append(token)
    if kept:
        return " / ".join(kept)

    compact = re.sub(r"\s+", "", text)
    if compact in NARRATOR_ROLES:
        return compact
    return normalize(text)


def clean_role_names(value: object) -> str:
    seen: set[str] = set()
    out: list[str] = []
    for part in re.split(r"/", str(value or "")):
        item = re.sub(r"\s+", "", part or "")
        if not item or item == "报幕" or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return " / ".join(out)


def first_sound_id(info: dict) -> str:
    episodes = (info.get("episodes") or {}).get("episode") or []
    for ep in episodes:
        sound_id = str(ep.get("sound_id") or "").strip()
        if sound_id:
            return sound_id
    return ""


def preview_sound_id(info: dict) -> str:
    sound_ids = preview_sound_ids(info)
    return sound_ids[0] if sound_ids else ""


def preview_sound_ids(info: dict) -> list[str]:
    out: list[str] = []
    episodes = (info.get("episodes") or {}).get("episode") or []
    for ep in episodes:
        title = normalize(ep.get("soundstr") or ep.get("name") or ep.get("title"))
        sound_id = str(ep.get("sound_id") or "").strip()
        if not sound_id:
            continue
        lowered = title.casefold()
        if ("预告" in title or "preview" in lowered) and sound_id not in out:
            out.append(sound_id)
    return out


def first_main_episode_sound_id(info: dict) -> str:
    episodes = (info.get("episodes") or {}).get("episode") or []
    for ep in episodes:
        title = ep.get("soundstr") or ep.get("name") or ep.get("title")
        sound_id = str(ep.get("sound_id") or "").strip()
        if sound_id and match_main_episode(title):
            return sound_id
    return ""


def preferred_sound_id(info: dict) -> tuple[str, bool]:
    preview_id = preview_sound_id(info)
    if preview_id:
        return preview_id, True
    return first_sound_id(info), False


def all_sound_ids(info: dict) -> list[str]:
    out: list[str] = []
    episodes = (info.get("episodes") or {}).get("episode") or []
    for ep in episodes:
        sound_id = str(ep.get("sound_id") or "").strip()
        if sound_id and sound_id not in out:
            out.append(sound_id)
    return out


def extract_label_names(category_labels: Iterable[dict]) -> list[str]:
    out: list[str] = []
    for item in category_labels or []:
        name = normalize(item.get("name"))
        if name and name not in out:
            out.append(name)
    return out


def infer_type_from_labels(category_labels: Iterable[dict]) -> int:
    labels = set(extract_label_names(category_labels))
    if "纯爱" in labels:
        return 4
    if "言情" in labels:
        return 6
    if "全年龄" in labels:
        return 3
    return 3


def is_narrator_role(role_name: object) -> bool:
    return clean_role_name(role_name) in NARRATOR_ROLES


def build_missevan_cv_entries(info: dict) -> list[dict]:
    entries: list[dict] = []
    for idx, item in enumerate(info.get("cvs") or []):
        cv_info = item.get("cv_info") or {}
        cv_id = cv_info.get("id")
        if cv_id in (None, ""):
            continue
        entries.append(
            {
                "index": idx,
                "cv_id": int(cv_id),
                "display_name": normalize(cv_info.get("name")),
                "role_name": clean_role_name(item.get("character")),
                "raw_role_name": normalize(item.get("character")),
            }
        )
    return entries


def build_missevan_main_cv_entries(info: dict) -> list[dict]:
    entries: list[dict] = []
    for idx, item in enumerate(info.get("cvs") or []):
        cv_info = item.get("cv_info") or {}
        is_main = safe_int(cv_info.get("main")) == 1 or safe_int(item.get("main")) == 1
        if not is_main:
            continue
        cv_id = cv_info.get("id")
        if cv_id in (None, ""):
            continue
        entries.append(
            {
                "index": idx,
                "cv_id": int(cv_id),
                "display_name": normalize(cv_info.get("name")),
                "role_name": clean_role_name(item.get("character")),
                "raw_role_name": normalize(item.get("character")),
            }
        )
    return entries


def build_manbo_cv_entries(payload: dict) -> list[dict]:
    entries: list[dict] = []
    for idx, item in enumerate(payload.get("cvRespList") or []):
        if safe_int(item.get("dramaRoleType")) != 2:
            continue
        cv_resp = item.get("cvResp") or {}
        cv_id = cv_resp.get("id")
        if cv_id in (None, ""):
            cv_id = item.get("platUid")
        if cv_id in (None, ""):
            continue
        entries.append(
            {
                "index": idx,
                "cv_id": int(cv_id),
                "display_name": normalize(item.get("cvNickname") or cv_resp.get("nickname")),
                "role_name": clean_role_name(item.get("role")),
                "raw_role_name": normalize(item.get("role")),
            }
        )
    return entries


def select_main_cv_entries(entries: list[dict], drama_type: int) -> list[dict]:
    limit = 4 if int(drama_type or 0) == 3 else 2
    if limit == 4:
        return entries[:4]

    selected: list[dict] = []
    for entry in entries:
        if is_narrator_role(entry.get("role_name")):
            continue
        selected.append(entry)
        if len(selected) >= limit:
            return selected

    for entry in entries:
        if entry not in selected:
            selected.append(entry)
            if len(selected) >= limit:
                break
    return selected[:limit]


def match_main_episode(title: object) -> bool:
    text = normalize_text_for_match(title)
    if not text:
        return False
    lowered = text.casefold()
    if any(keyword in lowered for keyword in NON_MAIN_EPISODE_KEYWORDS):
        return False
    return any(pattern.search(text) for pattern in EPISODE_PATTERNS)


def match_theme_song(title: object) -> bool:
    text = normalize_text_for_match(title)
    if not text:
        return False
    lowered = text.casefold()
    if any(keyword in lowered for keyword in ("预告", "番外")):
        return False
    return any(keyword in text for keyword in THEME_SONG_KEYWORDS)


def pick_first_episode_month(entries: list[dict], *, title_key: str, time_key: str, milliseconds: bool) -> str:
    candidates: list[tuple[int, float, str]] = []
    theme_candidates: list[tuple[int, float, str]] = []
    for idx, item in enumerate(entries):
        title = item.get(title_key)
        order_value = safe_int(item.get("order"), 0) or safe_int(item.get("setNo"), 0) or idx + 1
        timestamp_value = float(item.get(time_key) or 0)
        month = to_beijing_month(timestamp_value, milliseconds=milliseconds)
        if not month:
            continue
        if match_main_episode(title):
            candidates.append((order_value, timestamp_value, month))
            continue
        if match_theme_song(title):
            theme_candidates.append((order_value, timestamp_value, month))
    if not candidates:
        if not theme_candidates:
            return ""
        theme_candidates.sort(key=lambda item: (item[0], item[1]))
        return theme_candidates[0][2]
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][2]


def strip_catalog_suffix(title: object) -> str:
    text = normalize(title)
    if not text:
        return ""
    patterns = [
        r"（广播剧）$",
        r"（有声剧）$",
        r"（有声书）$",
        r"（有声漫）$",
        r"广播剧$",
        r"有声剧$",
        r"有声书$",
        r"有声漫$",
    ]
    for pattern in patterns:
        updated = re.sub(pattern, "", text)
        if updated != text:
            return normalize(updated)
    return text


def append_catalog_suffix(base_title: str, catalog_name: str) -> str:
    suffix = CATALOG_SUFFIX_BY_NAME.get(normalize(catalog_name))
    if not suffix:
        return normalize(base_title)
    base = normalize(base_title)
    if not base:
        return ""
    if base.endswith(f"（{suffix}）") or base.endswith(suffix):
        return base
    return f"{base}（{suffix}）"


def finalize_series_titles(records: list[dict], *, title_key: str, catalog_key: str, catalog_name_key: str, output_key: str) -> None:
    collision_map: dict[str, set[int]] = {}
    for record in records:
        base_title = strip_catalog_suffix(record.get(title_key))
        catalog = record.get(catalog_key)
        if not base_title or catalog in (None, ""):
            continue
        collision_map.setdefault(normalize_match(base_title), set()).add(int(catalog))

    for record in records:
        raw_title = normalize(record.get(title_key))
        base_title = strip_catalog_suffix(raw_title)
        catalog = record.get(catalog_key)
        catalog_name = normalize(record.get(catalog_name_key))
        final_title = raw_title
        catalog_set = collision_map.get(normalize_match(base_title), set())
        if len(catalog_set) > 1:
            if catalog_name == "广播剧" or safe_int(catalog) in (1, 89):
                final_title = base_title
            else:
                final_title = append_catalog_suffix(base_title, catalog_name)
        record[output_key] = normalize(final_title)


class MissevanRequester:
    def __init__(self, *, base_delay: float = 2.6, jitter: float = 1.2, max_retries: int = 3) -> None:
        self.base_delay = base_delay
        self.jitter = jitter
        self.max_retries = max_retries
        self.request_count = 0
        self.last_backoff_seconds = 0.0

    def request_json(self, url: str) -> dict:
        attempt = 0
        while True:
            time.sleep(self.base_delay + random.random() * self.jitter)
            response = requests.get(url, headers=MISSEVAN_HEADERS, timeout=30)
            self.request_count += 1
            if response.status_code != 418:
                response.raise_for_status()
                self.last_backoff_seconds = 0.0
                return response.json()
            attempt += 1
            if attempt > self.max_retries:
                raise RuntimeError("HTTP_418")
            backoff = min(120.0, 12.0 * (2 ** (attempt - 1)) + random.random() * 5.0)
            self.last_backoff_seconds = backoff
            time.sleep(backoff)


def request_manbo_json(url: str) -> dict:
    response = requests.get(url, headers=MANBO_HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()
