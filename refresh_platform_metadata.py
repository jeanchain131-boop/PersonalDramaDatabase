from __future__ import annotations

import re
import sqlite3
from copy import deepcopy

from clean_manbo_pricing import MANBO_PRICING_EXCLUSIONS, classify_manbo_pricing
from cvid_map_tools import load_combined_map
from platform_sync import (
    GENRE_BY_TYPE,
    MANBO_CATALOG_NAME_ALIASES,
    MANBO_CATALOG_NAME_BY_ID,
    MANBO_COUNTS_PATH,
    MANBO_INFO_PATH,
    MERGED_PATH,
    MISSEVAN_CATALOG_NAME_BY_ID,
    MISSEVAN_COUNTS_PATH,
    MISSEVAN_INFO_PATH,
    SQLITE_PATH,
    all_sound_ids,
    build_manbo_cv_entries,
    build_missevan_main_cv_entries,
    build_missevan_cv_entries,
    finalize_series_titles,
    first_main_episode_sound_id,
    finalize_missevan_store,
    infer_type_from_labels,
    iter_missevan_nodes,
    load_cache,
    load_json,
    normalize,
    pick_first_episode_month,
    preferred_sound_id,
    request_manbo_json,
    remove_missevan_node as remove_missevan_store_node,
    safe_int,
    save_cache,
    save_json,
    save_missevan_store,
    select_main_cv_entries,
    split_csv,
    utc_now,
    MissevanRequester,
    preview_sound_ids,
)


MISSEVAN_BLOCKLIST = {"47639", "25812"}
MISSEVAN_ARCHIVED_INFO_PATH = MISSEVAN_INFO_PATH.with_name("missevan-archived-drama.json")
MANBO_CATALOG_OVERRIDES = {
    "奇洛李维斯回信": {"catalog": 5, "catalogName": "有声剧"},
}
MANBO_MAINCV_OVERRIDES = {
    "1653464054477357115": {
        "ids": [1793456226309, 1942055112768, 2664739041286],  # 宫墙柳 + 魏超
        "extras": {
            2664739041286: {"display_name": "魏超", "role_name": ""},
        },
    },
    "2069403049792634966": {
        "ids": [1842233442307, 3053261148187],  # 木偶综合征
    },
}
MISSEVAN_MAINCV_OVERRIDES = {
    "71321": [1759, 2048, 3177, 6957, 3349, 3427, 4940],  # 再世权臣 第一季
    "79826": [1759, 3349, 4940, 6957, 2048, 3427, 3177],  # 再世权臣 第二季
    "86827": [1759, 3427, 3349, 4940, 2048, 3177, 6957],  # 再世权臣 第三季（上）
    "90066": {
        "ids": [755, 3361, 3093, 3353, 3356, 1122],
        "extras": {
            755: {"display_name": "姜广涛", "role_name": "老板"},
            3361: {"display_name": "袁铭喆", "role_name": "医生"},
            3093: {"display_name": "郑希", "role_name": "扶苏"},
            3353: {"display_name": "文森", "role_name": "陆子冈"},
            3356: {"display_name": "胡良伟", "role_name": "胡亥"},
            1122: {"display_name": "倔强的小红", "role_name": "赵高"},
        },
    },  # 哑舍 第三册
    "91220": {
        "ids": [755, 3361, 3093, 3353, 3356, 1122],
        "extras": {
            755: {"display_name": "姜广涛", "role_name": "老板"},
            3361: {"display_name": "袁铭喆", "role_name": "医生"},
            3093: {"display_name": "郑希", "role_name": "扶苏"},
            3353: {"display_name": "文森", "role_name": "陆子冈"},
            3356: {"display_name": "胡良伟", "role_name": "胡亥"},
            1122: {"display_name": "倔强的小红", "role_name": "赵高"},
        },
    },  # 哑舍 第四册
    "92479": {
        "ids": [755, 3361, 3093, 3353, 3356, 1122],
        "extras": {
            755: {"display_name": "姜广涛", "role_name": "老板"},
            3361: {"display_name": "袁铭喆", "role_name": "医生"},
            3093: {"display_name": "郑希", "role_name": "扶苏"},
            3353: {"display_name": "文森", "role_name": "陆子冈"},
            3356: {"display_name": "胡良伟", "role_name": "胡亥"},
            1122: {"display_name": "倔强的小红", "role_name": "赵高"},
        },
    },  # 哑舍 第五册
}


def build_manbo_cv_name_map() -> dict[int, str]:
    mapped: dict[int, str] = {}
    for payload in load_combined_map().values():
        manbo_id = payload.get("manboCvId")
        display_name = normalize(payload.get("displayName"))
        if manbo_id in (None, ""):
            continue
        if not display_name:
            continue
        mapped.setdefault(int(manbo_id), display_name)
    return mapped


def build_missevan_cv_name_map() -> dict[int, str]:
    mapped: dict[int, str] = {}
    for payload in load_combined_map().values():
        msv_id = payload.get("missevanCvId") or payload.get("cvId")
        display_name = normalize(payload.get("displayName"))
        if msv_id in (None, ""):
            continue
        if not display_name:
            continue
        mapped.setdefault(int(msv_id), display_name)
    return mapped


_missevan_cv_name_map_cache: dict[int, str] | None = None


def _get_missevan_cv_name_map() -> dict[int, str]:
    global _missevan_cv_name_map_cache
    if _missevan_cv_name_map_cache is None:
        _missevan_cv_name_map_cache = build_missevan_cv_name_map()
    return _missevan_cv_name_map_cache


MANBO_AUTHOR_PATTERNS = [
    re.compile(r"(?:晋江文学城|长佩文学)\s*(?P<author>[^，。；：:、,.!?！？\r\n]{1,40}?)\s*原(?:著|作)"),
    re.compile(r"(?P<author>[^，。；：:、,.!?！？\r\n]{1,40}?)\s*原(?:著|作)"),
]
MANBO_AUTHOR_NOISE_TOKENS = (
    "漫播",
    "APP",
    "app",
    "联合出品",
    "携手",
    "出品",
    "广播剧",
    "有声剧",
    "有声书",
    "有声漫",
)
MANBO_AUTHOR_SPLIT_PATTERN = re.compile(r"[\r\n|｜]+|[。！？!?]+")
MANBO_AUTHOR_STRIP_CHARS = "《》“”\"'‘’「」『』【】[]（）()<>〈〉·•-—:：;；,，。！？!?"
MANBO_AUTHOR_SOURCE_PREFIXES = (
    "晋江文学城",
    "晋江文学",
    "长佩文学",
    "豆瓣阅读",
    "豆腐阅读",
    "布咕阅读",
    "燎原阅读",
    "星悦文化",
    "酷威文化",
    "快看",
)


def normalize_manbo_catalog_name(value: object) -> str:
    name = normalize(value)
    return MANBO_CATALOG_NAME_ALIASES.get(name, name)


def clean_manbo_author_candidate(value: object) -> str:
    candidate = normalize(value)
    if not candidate:
        return ""
    candidate = re.sub(r"^(?:改编自|原名|小说|作品)\s*", "", candidate)
    prefix_pattern = "|".join(re.escape(item) for item in MANBO_AUTHOR_SOURCE_PREFIXES)
    candidate = re.sub(rf"^(?:{prefix_pattern})\s*", "", candidate)
    candidate = candidate.strip(MANBO_AUTHOR_STRIP_CHARS)
    candidate = normalize(candidate)
    candidate = re.sub(r"\s*(?:系列|作品|出品|制作|独家播出|独播).*$", "", candidate)
    candidate = re.sub(r"^[^A-Za-z0-9\u4e00-\u9fff]+", "", candidate)
    candidate = candidate.strip(MANBO_AUTHOR_STRIP_CHARS)
    candidate = normalize(candidate)
    if not candidate:
        return ""
    if len(candidate) > 20:
        return ""
    if any(token in candidate for token in MANBO_AUTHOR_NOISE_TOKENS):
        return ""
    if any(ch in candidate for ch in ("\n", "\r", "|", "｜")):
        return ""
    return candidate


def extract_manbo_author(desc: object) -> str:
    text = normalize(desc)
    if not text:
        return ""

    segments = [normalize(part) for part in MANBO_AUTHOR_SPLIT_PATTERN.split(str(desc or ""))]
    segments = [part for part in segments if part]
    for segment in segments or [text]:
        for pattern in MANBO_AUTHOR_PATTERNS:
            for match in pattern.finditer(segment):
                candidate = clean_manbo_author_candidate(match.group("author"))
                if candidate:
                    return candidate
    return ""


def _apply_missevan_maincv_override(drama_id: str, entries: list[dict], main_entries: list[dict]) -> list[dict]:
    override = MISSEVAN_MAINCV_OVERRIDES.get(drama_id)
    if not override:
        return main_entries
    if isinstance(override, list):
        override_ids = override
        extras = {}
    else:
        override_ids = override.get("ids") or []
        extras = override.get("extras") or {}
    indexed = {int(entry["cv_id"]): entry for entry in entries}
    overridden: list[dict] = []
    for cv_id in override_ids:
        if cv_id in indexed:
            overridden.append(indexed[cv_id])
            continue
        extra = extras.get(int(cv_id))
        if extra:
            overridden.append(
                {
                    "index": len(overridden),
                    "cv_id": int(cv_id),
                    "display_name": normalize(extra.get("display_name")),
                    "role_name": normalize(extra.get("role_name")),
                    "raw_role_name": normalize(extra.get("role_name")),
                }
            )
    return overridden or main_entries


def _missevan_cv_maps(main_entries: list[dict], missevan_cv_name_map: dict[int, str] | None = None) -> tuple[dict[str, str], dict[str, str]]:
    cvroles: dict[str, str] = {}
    cvnames: dict[str, str] = {}
    for entry in main_entries:
        cv_id = str(entry["cv_id"])
        cvnames[cv_id] = (missevan_cv_name_map or {}).get(int(entry["cv_id"]), entry["display_name"])
        if entry["role_name"]:
            cvroles[cv_id] = entry["role_name"]
    return cvroles, cvnames


def build_missevan_base_node(info: dict, drama_type: int | None) -> tuple[dict, list[dict]]:
    drama = info.get("drama") or {}
    drama_id = str(drama.get("id") or "").strip()
    entries = build_missevan_cv_entries(info)
    main_entries = select_main_cv_entries(entries, int(drama_type or 0))
    main_entries = _apply_missevan_maincv_override(drama_id, entries, main_entries)
    cvroles, cvnames = _missevan_cv_maps(main_entries, _get_missevan_cv_name_map())
    return {
        "title": normalize(drama.get("name")),
        "dramaId": int(drama["id"]),
        "soundIds": all_sound_ids(info),
        "maincvs": [int(entry["cv_id"]) for entry in main_entries],
        "type": None if drama_type is None else int(drama_type),
        "cvroles": cvroles,
        "cvnames": cvnames,
        "catalog": None if drama.get("catalog") in (None, "") else int(drama["catalog"]),
        "createTime": "",
        "author": normalize(drama.get("author")),
        "needpay": safe_int(drama.get("pay_type")) != 0 and safe_int(drama.get("price")) > 0,
    }, entries


def apply_missevan_preview_maincvs(node: dict, drama_id: str, base_entries: list[dict], preview_info: dict, use_preview_maincvs: bool) -> dict:
    if not use_preview_maincvs:
        return node
    return apply_missevan_sound_maincvs(node, drama_id, base_entries, preview_info)


def apply_missevan_sound_maincvs(node: dict, drama_id: str, base_entries: list[dict], sound_info: dict) -> dict:
    preview_entries = build_missevan_main_cv_entries(sound_info)
    if not preview_entries:
        return node
    return apply_missevan_main_cv_entries(node, drama_id, base_entries, preview_entries)


def merge_missevan_main_cv_entries(sound_infos: list[dict]) -> list[dict]:
    merged: list[dict] = []
    seen_cv_ids: set[int] = set()
    for sound_info in sound_infos:
        for entry in build_missevan_main_cv_entries(sound_info):
            cv_id = int(entry["cv_id"])
            if cv_id in seen_cv_ids:
                continue
            seen_cv_ids.add(cv_id)
            merged.append(entry)
    return merged


def append_unique_missevan_main_cv_entries(target: list[dict], entries: list[dict], seen_cv_ids: set[int], stop_at: int | None = None) -> None:
    for entry in entries:
        if stop_at is not None and len(target) >= stop_at:
            return
        cv_id = int(entry["cv_id"])
        if cv_id in seen_cv_ids:
            continue
        seen_cv_ids.add(cv_id)
        target.append(entry)


def merge_missevan_min_two_main_cv_entries(
    preview_sound_infos: list[dict],
    first_episode_sound_info: dict,
    base_entries: list[dict],
    drama_type: int | None,
) -> list[dict]:
    merged: list[dict] = []
    seen_cv_ids: set[int] = set()
    append_unique_missevan_main_cv_entries(
        merged,
        merge_missevan_main_cv_entries(preview_sound_infos),
        seen_cv_ids,
    )
    if len(merged) >= 2:
        return merged

    append_unique_missevan_main_cv_entries(
        merged,
        build_missevan_main_cv_entries(first_episode_sound_info),
        seen_cv_ids,
    )
    if len(merged) >= 2:
        return merged

    append_unique_missevan_main_cv_entries(
        merged,
        select_main_cv_entries(base_entries, int(drama_type or 0)),
        seen_cv_ids,
        stop_at=2,
    )
    return merged


def apply_missevan_main_cv_entries(node: dict, drama_id: str, base_entries: list[dict], main_entries: list[dict]) -> dict:
    if not main_entries:
        return node
    final_entries = _apply_missevan_maincv_override(drama_id, base_entries, main_entries)
    cvroles, cvnames = _missevan_cv_maps(final_entries, _get_missevan_cv_name_map())
    updated_node = dict(node)
    updated_node["maincvs"] = [int(entry["cv_id"]) for entry in final_entries]
    updated_node["cvroles"] = cvroles
    updated_node["cvnames"] = cvnames
    return updated_node


def apply_missevan_merged_sound_maincvs(node: dict, drama_id: str, base_entries: list[dict], sound_infos: list[dict]) -> dict:
    preview_entries = merge_missevan_main_cv_entries(sound_infos)
    if not preview_entries:
        return node
    return apply_missevan_main_cv_entries(node, drama_id, base_entries, preview_entries)


def resolve_missevan_type(raw_type: object) -> int:
    if isinstance(raw_type, str):
        type_name = normalize(raw_type)
        if type_name == "全年龄":
            return 3
        if type_name == "纯爱":
            return 4
        if type_name == "言情":
            return 6
        try:
            return int(type_name)
        except ValueError:
            return 3
    try:
        return int(raw_type or 0)
    except (TypeError, ValueError):
        return 3


def finalize_missevan_store_titles(store: dict) -> dict:
    finalized, _conflicts = finalize_missevan_store(store)
    return finalized


def is_http_403(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None) == 403


def archive_missevan_node(archive: dict, series_title: str, season_key: str, node: dict, watch_count: dict | None) -> None:
    archived_node = deepcopy(node)
    archived_node["archivedReason"] = "HTTP_403"
    archived_node["archivedAt"] = utc_now()
    archived_node["archivedWatchCount"] = deepcopy(watch_count)
    archive.setdefault(series_title, {})[season_key] = archived_node


def remove_missevan_node(store: dict, series_title: str, season_key: str) -> None:
    remove_missevan_store_node(store, series_title, season_key)


def refresh_missevan(*, target_drama_ids: set[str] | None = None, force: bool = True, update_counts: bool = True, all_age_only: bool = False) -> dict:
    store = load_json(MISSEVAN_INFO_PATH, {})
    archive = load_json(MISSEVAN_ARCHIVED_INFO_PATH, {})
    cache = load_cache(MISSEVAN_COUNTS_PATH) if update_counts else {"_meta": {"updated_at": None}, "counts": {}}
    requester = MissevanRequester()
    unknown_catalogs: set[int] = set()
    missing_catalog = 0
    processed = 0
    skipped = 0
    archived = 0

    pending_nodes: list[tuple[str, str, dict]] = []
    pending_nodes = list(iter_missevan_nodes(store))

    for idx, (series_title, season_key, node) in enumerate(pending_nodes, start=1):
        drama_id = str(node.get("dramaId") or "").strip()
        if not drama_id or drama_id in MISSEVAN_BLOCKLIST:
            continue
        if target_drama_ids is not None and drama_id not in target_drama_ids:
            continue
        if all_age_only and int(node.get("type") or 0) != 3:
            continue
        cached = (cache.get("counts") or {}).get(drama_id) or {}
        if (
            not force
            and "catalog" in node
            and "createTime" in node
            and node.get("maincvs") is not None
            and "author" in node
            and "needpay" in node
            and (not update_counts or cached.get("view_count") is not None)
        ):
            skipped += 1
            continue

        try:
            drama_payload = requester.request_json(f"https://www.missevan.com/dramaapi/getdrama?drama_id={drama_id}")
        except RuntimeError:
            save_missevan_store(MISSEVAN_INFO_PATH, store)
            save_json(MISSEVAN_ARCHIVED_INFO_PATH, archive)
            if update_counts:
                save_cache(MISSEVAN_COUNTS_PATH, cache)
            raise
        except Exception as exc:
            if is_http_403(exc):
                watch_count = (cache.get("counts") or {}).get(drama_id)
                archive_missevan_node(archive, series_title, season_key, node, watch_count)
                remove_missevan_node(store, series_title, season_key)
                cache.get("counts", {}).pop(drama_id, None)
                save_missevan_store(MISSEVAN_INFO_PATH, store)
                save_json(MISSEVAN_ARCHIVED_INFO_PATH, archive)
                if update_counts:
                    save_cache(MISSEVAN_COUNTS_PATH, cache)
                archived += 1
                print(f"[猫耳] 403归档 ID={drama_id} {season_key} title={normalize(node.get('title') or series_title)}")
                continue
            save_missevan_store(MISSEVAN_INFO_PATH, store)
            save_json(MISSEVAN_ARCHIVED_INFO_PATH, archive)
            if update_counts:
                save_cache(MISSEVAN_COUNTS_PATH, cache)
            print(
                "Failed while refreshing 猫耳 metadata. "
                f"Progress has been saved. dramaId={drama_id} title={normalize(node.get('title') or series_title)} "
                f"error={type(exc).__name__}: {exc}"
            )
            raise
        info = (drama_payload or {}).get("info") or {}
        drama = info.get("drama") or {}
        sound_id, used_preview_sound = preferred_sound_id(info)
        preview_sound_id_list = preview_sound_ids(info)
        first_episode_sound_id = first_main_episode_sound_id(info)
        current_type = node.get("type")
        drama_type: int | None = None if current_type in (None, "") else int(current_type)
        create_month = normalize(node.get("createTime"))
        sound_info: dict = {}
        preview_sound_infos: list[dict] = []
        should_fetch_sound = bool(sound_id) and (used_preview_sound or drama_type is None or not create_month)
        if should_fetch_sound and sound_id:
            target_sound_ids = preview_sound_id_list if used_preview_sound and preview_sound_id_list else [sound_id]
            for fetch_sound_id in target_sound_ids:
                try:
                    sound_payload = requester.request_json(f"https://www.missevan.com/dramaapi/getdramabysound?sound_id={fetch_sound_id}")
                except RuntimeError:
                    save_missevan_store(MISSEVAN_INFO_PATH, store)
                    save_json(MISSEVAN_ARCHIVED_INFO_PATH, archive)
                    if update_counts:
                        save_cache(MISSEVAN_COUNTS_PATH, cache)
                    raise
                fetched_sound_info = (sound_payload or {}).get("info") or {}
                if not sound_info:
                    sound_info = fetched_sound_info
                preview_sound_infos.append(fetched_sound_info)
            sound_drama = sound_info.get("drama") or {}
            episodes = (sound_info.get("episodes") or {}).get("episode") or []
            if drama_type is None:
                drama_type = resolve_missevan_type(sound_drama.get("type"))
            if not create_month:
                create_month = pick_first_episode_month(episodes, title_key="name", time_key="create_time", milliseconds=False)
        updated_node, base_entries = build_missevan_base_node(info, drama_type)
        maincv_preview_sound_infos = preview_sound_infos if used_preview_sound else []
        preview_main_entries = merge_missevan_main_cv_entries(maincv_preview_sound_infos)
        should_fill_two_maincvs = int(drama_type or 0) in (4, 6)
        episode_sound_info: dict = {}
        has_sound_maincvs = bool(preview_main_entries)
        should_try_first_episode_maincvs = bool(first_episode_sound_id) and (
            (should_fill_two_maincvs and len(preview_main_entries) < 2) or (not used_preview_sound or not has_sound_maincvs)
        )
        if should_try_first_episode_maincvs:
            episode_sound_info = sound_info if first_episode_sound_id == sound_id and sound_info else {}
            if not episode_sound_info:
                try:
                    episode_sound_payload = requester.request_json(f"https://www.missevan.com/dramaapi/getdramabysound?sound_id={first_episode_sound_id}")
                except RuntimeError:
                    save_missevan_store(MISSEVAN_INFO_PATH, store)
                    save_json(MISSEVAN_ARCHIVED_INFO_PATH, archive)
                    if update_counts:
                        save_cache(MISSEVAN_COUNTS_PATH, cache)
                    raise
                episode_sound_info = (episode_sound_payload or {}).get("info") or {}
        if should_fill_two_maincvs:
            merged_main_entries = merge_missevan_min_two_main_cv_entries(maincv_preview_sound_infos, episode_sound_info, base_entries, drama_type)
            updated_node = apply_missevan_main_cv_entries(updated_node, drama_id, base_entries, merged_main_entries)
        else:
            updated_node = apply_missevan_merged_sound_maincvs(updated_node, drama_id, base_entries, maincv_preview_sound_infos) if used_preview_sound else updated_node
            updated_node = apply_missevan_sound_maincvs(updated_node, drama_id, base_entries, episode_sound_info)
        updated_node["createTime"] = create_month

        catalog = updated_node.get("catalog")
        if catalog is None:
            missing_catalog += 1
        elif int(catalog) not in set(MISSEVAN_CATALOG_NAME_BY_ID):
            unknown_catalogs.add(int(catalog))

        if update_counts:
            cache["counts"][drama_id] = {
                "name": normalize(drama.get("name") or updated_node.get("title") or series_title),
                "view_count": None if drama.get("view_count") is None else int(drama["view_count"]),
                "fetched_at": utc_now(),
            }
        store[str(drama_id)] = updated_node
        processed += 1
        if processed % 10 == 0 or idx == len(pending_nodes):
            save_missevan_store(MISSEVAN_INFO_PATH, store)
            save_json(MISSEVAN_ARCHIVED_INFO_PATH, archive)
            if update_counts:
                save_cache(MISSEVAN_COUNTS_PATH, cache)

    save_missevan_store(MISSEVAN_INFO_PATH, store)
    save_json(MISSEVAN_ARCHIVED_INFO_PATH, archive)
    if update_counts:
        save_cache(MISSEVAN_COUNTS_PATH, cache)
    return {
        "processed": processed,
        "count_entries_updated": processed if update_counts else 0,
        "skipped": skipped,
        "unknown_catalogs": sorted(unknown_catalogs),
        "missing_catalog": missing_catalog,
        "archived": archived,
        "last_backoff_seconds": requester.last_backoff_seconds,
        "request_count": requester.request_count,
    }


def upsert_missevan_drama_ids(drama_ids: list[str], *, force: bool = True) -> dict:
    store = load_json(MISSEVAN_INFO_PATH, {})
    existing_ids = {str(node.get("dramaId") or "").strip() for _series_title, _season_key, node in iter_missevan_nodes(store)}
    for drama_id in drama_ids:
        drama_id = str(drama_id).strip()
        if drama_id in existing_ids:
            continue
        title_key = f"__pending__{drama_id}"
        store[drama_id] = {"dramaId": int(drama_id), "title": title_key}
    save_missevan_store(MISSEVAN_INFO_PATH, store)
    return refresh_missevan(target_drama_ids={str(item) for item in drama_ids}, force=force)


def build_manbo_record(record: dict, payload: dict, manbo_cv_name_map: dict[int, str] | None = None) -> dict:
    data = payload.get("data") or {}
    catalog = data.get("catelog")
    if catalog is None:
        catalog = data.get("category")
    category_resp = data.get("radioDramaCategoryResp") or {}
    labels = data.get("categoryLabels") or []
    drama_type = infer_type_from_labels(labels)
    entries = build_manbo_cv_entries(data)
    main_entries = select_main_cv_entries(entries, drama_type)
    override_main = MANBO_MAINCV_OVERRIDES.get(str(record.get("dramaId") or ""))
    if override_main:
        indexed = {int(entry["cv_id"]): entry for entry in entries}
        extras = override_main.get("extras") or {}
        main_entries = []
        for cv_id in override_main.get("ids") or []:
            if cv_id in indexed:
                main_entries.append(indexed[cv_id])
                continue
            extra = extras.get(int(cv_id))
            if extra:
                main_entries.append(
                    {
                        "index": len(main_entries),
                        "cv_id": int(cv_id),
                        "display_name": normalize(extra.get("display_name")),
                        "role_name": normalize(extra.get("role_name")),
                        "raw_role_name": normalize(extra.get("role_name")),
                    }
                )
    override = MANBO_CATALOG_OVERRIDES.get(normalize(record.get("normalizedName") or record.get("name") or data.get("title")))
    if override is not None:
        final_catalog = int(override["catalog"])
        catalog_name = normalize_manbo_catalog_name(override["catalogName"])
    else:
        final_catalog = None if catalog in (None, "") else int(catalog)
        default_catalog_name = MANBO_CATALOG_NAME_BY_ID.get(int(final_catalog), "") if final_catalog is not None else ""
        catalog_name = normalize_manbo_catalog_name(category_resp.get("name") or default_catalog_name)

    updated = dict(record)
    updated["name"] = normalize(data.get("title") or record.get("name"))
    updated["normalizedName"] = normalize(record.get("normalizedName") or updated["name"]).casefold()
    updated["catalog"] = final_catalog
    updated["catalogName"] = catalog_name
    updated["type"] = drama_type
    updated["genre"] = GENRE_BY_TYPE.get(drama_type, "")
    updated["mainCvIds"] = [int(entry["cv_id"]) for entry in main_entries]
    updated["mainCvNames"] = [(manbo_cv_name_map or {}).get(cv_id, "") for cv_id in updated["mainCvIds"]]
    updated["mainCvNicknames"] = [entry["display_name"] for entry in main_entries]
    updated["mainCvRoleNames"] = [entry["role_name"] for entry in main_entries]
    updated["createTime"] = pick_first_episode_month(data.get("setRespList") or [], title_key="setTitle", time_key="createTime", milliseconds=True)
    updated["author"] = extract_manbo_author(data.get("desc"))
    drama_id = str(updated.get("dramaId") or record.get("dramaId") or "").strip()
    pricing_category = classify_manbo_pricing(payload or {})
    updated["needpay"] = drama_id in MANBO_PRICING_EXCLUSIONS or pricing_category not in {"free", "100_redbean"}
    return updated


def finalize_manbo_records(records: list[dict]) -> None:
    for record in records:
        catalog_name = normalize_manbo_catalog_name(record.get("catalogName"))
        if not catalog_name and record.get("catalog") not in (None, ""):
            record["catalogName"] = normalize_manbo_catalog_name(MANBO_CATALOG_NAME_BY_ID.get(int(record["catalog"]), ""))
        elif catalog_name:
            record["catalogName"] = catalog_name
    finalize_series_titles(records, title_key="name", catalog_key="catalog", catalog_name_key="catalogName", output_key="seriesTitle")


def refresh_manbo(*, target_drama_ids: set[str] | None = None, force: bool = True, all_age_only: bool = False) -> dict:
    info = load_json(MANBO_INFO_PATH, {"version": 1, "updatedAt": None, "records": []})
    records = info.get("records", [])
    cache = load_cache(MANBO_COUNTS_PATH)
    manbo_cv_name_map = build_manbo_cv_name_map()
    unknown_catalogs: set[int] = set()
    missing_catalog = 0
    processed = 0
    skipped = 0

    for idx, record in enumerate(records, start=1):
        drama_id = str(record.get("dramaId") or "").strip()
        if not drama_id:
            continue
        if target_drama_ids is not None and drama_id not in target_drama_ids:
            continue
        if all_age_only and int(record.get("type") or 0) != 3:
            continue
        cached = (cache.get("counts") or {}).get(drama_id) or {}
        if (
            not force
            and "catalog" in record
            and "catalogName" in record
            and "createTime" in record
            and record.get("mainCvIds") is not None
            and record.get("mainCvNames") is not None
            and "author" in record
            and "needpay" in record
            and cached.get("view_count") is not None
        ):
            skipped += 1
            continue

        payload = request_manbo_json(f"https://www.kilamanbo.world/web_manbo/dramaDetail?dramaId={drama_id}")
        updated = build_manbo_record(record, payload, manbo_cv_name_map)
        if updated.get("catalog") is None:
            missing_catalog += 1
        elif int(updated["catalog"]) not in set(MANBO_CATALOG_NAME_BY_ID):
            unknown_catalogs.add(int(updated["catalog"]))
        data = payload.get("data") or {}
        cache["counts"][drama_id] = {
            "name": normalize(data.get("title") or updated.get("name")),
            "view_count": None if data.get("watchCount") is None else int(data["watchCount"]),
            "fetched_at": utc_now(),
        }
        records[idx - 1] = updated
        processed += 1
        if processed % 25 == 0:
            finalize_manbo_records(records)
            info["updatedAt"] = utc_now()
            save_json(MANBO_INFO_PATH, info)
            save_cache(MANBO_COUNTS_PATH, cache)

    finalize_manbo_records(records)
    info["updatedAt"] = utc_now()
    save_json(MANBO_INFO_PATH, info)
    save_cache(MANBO_COUNTS_PATH, cache)
    return {
        "processed": processed,
        "skipped": skipped,
        "unknown_catalogs": sorted(unknown_catalogs),
        "missing_catalog": missing_catalog,
    }


def upsert_manbo_drama_ids(drama_ids: list[str], *, force: bool = True) -> dict:
    info = load_json(MANBO_INFO_PATH, {"version": 1, "updatedAt": None, "records": []})
    records = info.setdefault("records", [])
    existing = {str(item.get("dramaId") or "") for item in records}
    for drama_id in drama_ids:
        if str(drama_id) not in existing:
            records.append(
                {
                    "dramaId": str(drama_id),
                    "name": "",
                    "normalizedName": "",
                    "aliases": [],
                    "cover": "",
                    "author": "",
                    "mainCvNames": [],
                }
            )
    save_json(MANBO_INFO_PATH, info)
    return refresh_manbo(target_drama_ids={str(item) for item in drama_ids}, force=force)


def fix_chennianliegou(conn: sqlite3.Connection) -> None:
    missevan_cache = load_cache(MISSEVAN_COUNTS_PATH).get("counts", {})
    total = int((missevan_cache.get("61128") or {}).get("view_count") or 0) + int((missevan_cache.get("73251") or {}).get("view_count") or 0)
    conn.execute(
        """
        UPDATE cv_works
        SET dramaids_text = '61128,73251',
            total_play_count = ?
        WHERE platform = '猫耳' AND title = '陈年烈苟' AND cv_name IN ('苏尚卿', '凌飞')
        """,
        (total,),
    )


def refresh_sqlite_from_caches() -> None:
    missevan_counts = load_cache(MISSEVAN_COUNTS_PATH).get("counts", {})
    manbo_counts = load_cache(MANBO_COUNTS_PATH).get("counts", {})

    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    fix_chennianliegou(conn)
    rows = conn.execute("SELECT id, dramaids_text, platform FROM cv_works").fetchall()
    for row in rows:
        drama_ids = [did for did in split_csv(row["dramaids_text"]) if did not in MISSEVAN_BLOCKLIST]
        count_map = missevan_counts if row["platform"] == "猫耳" else manbo_counts
        total = 0
        missing = False
        for did in drama_ids:
            item = count_map.get(did)
            if not item or item.get("view_count") is None:
                missing = True
                break
            total += int(item["view_count"])
        dramaids_text = ",".join(drama_ids)
        conn.execute(
            "UPDATE cv_works SET dramaids_text = ?, total_play_count = ? WHERE id = ?",
            (dramaids_text, None if missing and drama_ids else total, row["id"]),
        )

    conn.execute("DELETE FROM work_drama_ids")
    fresh_rows = conn.execute("SELECT id, dramaids_text FROM cv_works").fetchall()
    for row in fresh_rows:
        for drama_id in split_csv(row["dramaids_text"]):
            conn.execute("INSERT INTO work_drama_ids(work_id, drama_id) VALUES (?, ?)", (row["id"], drama_id))
    conn.commit()
    conn.close()


def export_sqlite_to_workbook() -> None:
    from export_sqlite_to_workbook import build_workbook

    build_workbook()


def main() -> int:
    try:
        missevan_stats = refresh_missevan(force=True, all_age_only=True)
    except RuntimeError:
        print("Hit 418 while refreshing 猫耳 metadata. Progress has been saved.")
        return 2

    manbo_stats = refresh_manbo(force=True, all_age_only=True)
    refresh_sqlite_from_caches()
    export_sqlite_to_workbook()

    print("猫耳 seasons refreshed:", missevan_stats["processed"])
    print("猫耳 seasons skipped:", missevan_stats["skipped"])
    print("猫耳 seasons archived:", missevan_stats["archived"])
    print("猫耳 missing catalog:", missevan_stats["missing_catalog"])
    print("猫耳 unknown catalogs:", missevan_stats["unknown_catalogs"])
    print("猫耳 requests:", missevan_stats["request_count"])
    print("猫耳 recent backoff seconds:", missevan_stats["last_backoff_seconds"])
    print("漫播 records refreshed:", manbo_stats["processed"])
    print("漫播 records skipped:", manbo_stats["skipped"])
    print("漫播 missing catalog:", manbo_stats["missing_catalog"])
    print("漫播 unknown catalogs:", manbo_stats["unknown_catalogs"])
    print("Created:", MERGED_PATH.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
