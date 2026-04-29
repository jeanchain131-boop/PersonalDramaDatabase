# PersonalDramaDatabase Project Architecture

## Project Overview
Chinese audio drama database aggregating data from two platforms (Missevan/猫耳 and Manbo/漫播). Tracks metadata, play counts, CV (voice actor) relationships and generates ranking reports.

## Data Files Structure

### missevan-drama-info.json
- **Type**: Nested dictionary (series_title/season_key -> node)
- **Sample Entry**:
```json
{
  "2291": {
    "title": "午夜主持",
    "dramaId": 2291,
    "soundIds": ["60638"],
    "maincvs": [1727, 1286],
    "type": 4,  // 3=全年龄, 4=纯爱, 6=言情
    "cvroles": {"1727": "老者", "1286": "混混乙"},
    "cvnames": {"1727": "冰眼霜牙", "1286": "墨千临"},
    "catalog": 89,  // 89/90=广播剧, 93=有声剧, 96=有声漫
    "createTime": "2016.11",
    "author": "青丘",
    "needpay": false,
    "seriesTitle": "午夜主持"
  }
}
```

### manbo-drama-info.json
- **Type**: {"version": 1, "updatedAt": ISO string, "records": []}
- **Sample Entry**:
```json
{
  "dramaId": "1467142227078676553",
  "name": "神明今夜想你",
  "mainCvNicknames": ["轩ZONE", "乔诗语🌞729声工场"],
  "catalog": 1,  // 1=广播剧, 5=有声剧（有声书按有声剧处理）
  "createTime": "2020.11",
  "catalogName": "广播剧",
  "type": 6,
  "genre": "言情",
  "mainCvIds": [1875609632770, 1917412745234],
  "mainCvRoleNames": ["驰厌", "姜穗"],
  "seriesTitle": "神明今夜想你",
  "author": "藤萝为枝",
  "mainCvNames": ["轩ZONE", "乔诗语"],
  "needpay": true
}
```

### Watch Count Caches
**Structure**: `{"_meta": {"updated_at": ISO}, "counts": {drama_id: {...}}}`
**Entry**:
```json
{
  "name": "427侦探事务所",
  "view_count": 744113,
  "fetched_at": "2026-04-13T11:49:10+00:00"
}
```

### drama-series-info.json
- Links multiple drama IDs to series (handles multi-season works)
- Example: "猫耳:再世权臣" maps to dramIds: ["71321", "79826", "86827"]

## API Endpoints & Connection Patterns

### Missevan (猫耳) APIs
**Base**: `https://www.missevan.com/dramaapi/`
- **getdrama**: `https://www.missevan.com/dramaapi/getdrama?drama_id={drama_id}`
- **getdramabysound**: `https://www.missevan.com/dramaapi/getdramabysound?sound_id={sound_id}`

**Response Structure**:
```
{
  "info": {
    "drama": {
      "id", "name", "catalog", "author", "view_count", 
      "pay_type", "price", "type"
    },
    "cvs": [{"cv_info": {"id", "name", "main"}, "character", "main"}],
    "episodes": {"episode": [{"sound_id", "name", "create_time", ...}]}
  }
}
```

### Manbo (漫播) APIs
**URL Pattern**: `/app/radioDrama/get/{dramaId}` (via request_manbo_json)

**Response Structure**:
```
{
  "data": {
    "catelog" or "category": int,
    "radioDramaCategoryResp": {"name": string},
    "categoryLabels": [{"name": string}],
    "title": string,
    "cvRespList": [{
      "dramaRoleType": 2 (for main CVs),
      "cvResp": {"id", "nickname"},
      "cvNickname", "role", "platUid"
    }]
  }
}
```

## Rate Limiting & Throttling

### Missevan Requester (platform_sync.py:MissevanRequester)
```python
base_delay: float = 2.6  # Base delay in seconds
jitter: float = 1.2      # Random jitter (+/- 1.2s)

# 418 (Too Many Requests) handling:
# - Exponential backoff: 12.0 * (2^(attempt-1))
# - Max 3 retries
# - Max backoff: 120 seconds
# - Example: 12s → 24s → 48s (+ 5s random)
```

### Watch Count Cache Window
```python
CACHE_WINDOW = timedelta(hours=1)
# Skips refresh if fetched_at is within 1 hour
```

### Missevan Blocklist
- Excluded drama IDs: {"47639", "25812"}

## Upstash Redis Connection (sync_new_drama_ids.py)

**Configuration**:
```python
UPSTASH_REDIS_REST_URL      # Environment variable
UPSTASH_REDIS_REST_TOKEN    # Environment variable

def upstash_request(command: list[object]) -> object:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    # POST to REST endpoint with Redis command
```

**Queue Keys**:
- `new:dramaIDs` - JSON queue: `{"manbo": [ids], "missevan": [ids]}`
- `manbo:info:v1` - Uploaded manbo-drama-info.json
- `missevan:info:v1` - Uploaded missevan-drama-info.json
- `ongoing:missevan` / `ongoing:manbo` - JSON payloads from fetch_ongoing.py with ongoing drama IDs and updateType.
- `ranks:partial:{platform}` - Latest per-platform rank/drama shard used to merge partial updates.
- `ranks:metrics:{date}:{platform}` - Daily drama metric shard used to restore cached rank drama fields.

## Rank SQL Structure (DramaByCV.rank.sql)

**Key Components**:
- `cv_works` table: Aggregates plays per CV across both platforms
- Partitions: Group by cv_name, order by total_play_count DESC
- Outputs:
  - rank_no (DENSE_RANK)
  - total_play_count (SUM aggregation)
  - lead_count (number of works)
  - top3_titles (top 3 works by plays)

## Script Flow

### Append/Refresh Flow:
1. `append_missevan_ids.py {ids}` → calls `upsert_missevan_drama_ids()`
2. `refresh_missevan()` → fetches from API, enriches metadata
3. Updates: missevan-drama-info.json + missevan-watch-counts.json
4. `update_combined_cvid_map()` → syncs CV mapping

### Sync/Upload Flow:
1. `sync_new_drama_ids.py` → reads from Upstash queue
2. Calls append scripts for pending IDs
3. `upload_json_file()` → stores updated files in Upstash
4. Prunes completed IDs from queue

### Ongoing Rank Fetch Flow:
1. `fetch_ongoing.py` collects paid ongoing drama IDs from Missevan timeline/sound pages and Manbo update-time pages.
2. Uploads compact records to `ongoing:missevan` and `ongoing:manbo`; local `ongoing-*.json` files are dry-run snapshots only.
3. `fetch_rank_data.py` merges rank IDs with ongoing IDs before stale filtering, so ongoing dramas stay refreshed even when absent from rank lists.
4. Rank history uploads include `cover`, `maincvs`, `catalogName`, `payStatus`, and `createTime` in metrics shards so remote cache restores keep display metadata.

## Key Processing Functions

### CV Entry Selection:
- **All-age dramas (type=3)**: Take first 4 CVs
- **Romance/Pure Love (type=4,6)**: Take first 2 non-narrator CVs
- Overridable via `MISSEVAN_MAINCV_OVERRIDES` / `MANBO_MAINCV_OVERRIDES`
- New observed CV names can be appended to `missevan&manbo-cvid-map.json` during metadata/rank refreshes.

### Author Extraction:
- Manbo: Regex patterns match "原著/原作" attribution
- Missevan: Direct from drama.author field
