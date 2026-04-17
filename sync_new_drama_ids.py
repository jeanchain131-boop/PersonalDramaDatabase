from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import requests

from platform_sync import MANBO_INFO_PATH, MISSEVAN_INFO_PATH, iter_missevan_nodes, load_json, normalize


ROOT = Path(__file__).resolve().parent
QUEUE_KEY = "new:dramaIDs"
MANBO_INFO_KEY = "manbo:info:v1"
MISSEVAN_INFO_KEY = "missevan:info:v1"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        item = line.strip()
        if not item or item.startswith("#") or "=" not in item:
            continue
        key, value = item.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


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
    if not isinstance(data, dict):
        raise RuntimeError(f"{QUEUE_KEY} must be a JSON object.")
    return {
        "manbo": normalize_ids(data.get("manbo") or []),
        "missevan": normalize_ids(data.get("missevan") or []),
    }


def normalize_ids(values: list[object]) -> list[str]:
    out: list[str] = []
    for value in values:
        item = normalize(value)
        if item and item not in out:
            out.append(item)
    return out


def run_script(script_name: str, drama_ids: list[str]) -> None:
    if not drama_ids:
        print(f"[skip] {script_name}: no ids")
        return
    command = [sys.executable, "-X", "utf8", script_name, *drama_ids]
    print(f"$ {subprocess.list2cmdline(command)}")
    process = subprocess.Popen(
        command,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert process.stdout is not None
    for line in process.stdout:
        print(line, end="")
    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"{script_name} failed with exit code {return_code}")


def upload_json_file(key: str, path: Path) -> None:
    value = path.read_text(encoding="utf-8")
    result = upstash_request(["SET", key, value])
    if result != "OK":
        raise RuntimeError(f"Failed to upload {path.name} to {key}: {result!r}")
    print(f"[ok] uploaded {path.name} -> {key}")


def build_missevan_index(store: dict) -> dict[str, dict]:
    indexed: dict[str, dict] = {}
    for _series_title, _season_key, node in iter_missevan_nodes(store):
        drama_id = normalize(node.get("dramaId"))
        if drama_id and drama_id not in indexed:
            indexed[drama_id] = node
    return indexed


def build_manbo_index(store: dict) -> dict[str, dict]:
    indexed: dict[str, dict] = {}
    for record in store.get("records") or []:
        drama_id = normalize(record.get("dramaId"))
        if drama_id and drama_id not in indexed:
            indexed[drama_id] = record
    return indexed


def is_missevan_ready(record: dict | None) -> bool:
    if not record:
        return False
    if not normalize(record.get("title")):
        return False
    if record.get("type") in (None, ""):
        return False
    if record.get("catalog") in (None, ""):
        return False
    has_create_time = bool(normalize(record.get("createTime")))
    has_author = bool(normalize(record.get("author")))
    if not has_create_time and not has_author:
        return False
    return len(record.get("maincvs") or []) >= 2


def is_manbo_ready(record: dict | None) -> bool:
    if not record:
        return False
    if not normalize(record.get("name")):
        return False
    if record.get("catalog") in (None, ""):
        return False
    if not normalize(record.get("createTime")):
        return False
    if not normalize(record.get("genre")):
        return False
    return len(record.get("mainCvNicknames") or []) >= 2


def prune_queue(queue: dict[str, list[str]]) -> dict[str, list[str]]:
    missevan_store = load_json(MISSEVAN_INFO_PATH, {})
    manbo_store = load_json(MANBO_INFO_PATH, {"records": []})
    missevan_index = build_missevan_index(missevan_store)
    manbo_index = build_manbo_index(manbo_store)
    remaining_missevan = [drama_id for drama_id in queue.get("missevan", []) if not is_missevan_ready(missevan_index.get(drama_id))]
    remaining_manbo = [drama_id for drama_id in queue.get("manbo", []) if not is_manbo_ready(manbo_index.get(drama_id))]
    return {"manbo": remaining_manbo, "missevan": remaining_missevan}


def save_queue(queue: dict[str, list[str]]) -> None:
    payload = json.dumps(
        {
            "manbo": normalize_ids(queue.get("manbo") or []),
            "missevan": normalize_ids(queue.get("missevan") or []),
        },
        ensure_ascii=False,
    )
    result = upstash_request(["SET", QUEUE_KEY, payload])
    if result != "OK":
        raise RuntimeError(f"Failed to update {QUEUE_KEY}: {result!r}")
    print(
        "[ok] updated queue:",
        json.dumps(
            {
                "manbo": len(queue.get("manbo") or []),
                "missevan": len(queue.get("missevan") or []),
            },
            ensure_ascii=False,
        ),
    )


def main() -> int:
    load_env_file(ROOT / ".env")
    queue = load_queue()
    manbo_ids = queue.get("manbo") or []
    missevan_ids = queue.get("missevan") or []
    print(f"[queue] manbo={len(manbo_ids)} missevan={len(missevan_ids)}")
    if not manbo_ids and not missevan_ids:
        print("No pending drama IDs in new:dramaIDs.")
        return 0

    run_script("append_manbo_ids.py", manbo_ids)
    run_script("append_missevan_ids.py", missevan_ids)

    upload_json_file(MANBO_INFO_KEY, MANBO_INFO_PATH)
    upload_json_file(MISSEVAN_INFO_KEY, MISSEVAN_INFO_PATH)

    remaining_queue = prune_queue(queue)
    save_queue(remaining_queue)
    print(
        "[done]",
        json.dumps(
            {
                "removed_manbo": len(manbo_ids) - len(remaining_queue["manbo"]),
                "removed_missevan": len(missevan_ids) - len(remaining_queue["missevan"]),
                "remaining_manbo": len(remaining_queue["manbo"]),
                "remaining_missevan": len(remaining_queue["missevan"]),
            },
            ensure_ascii=False,
        ),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
