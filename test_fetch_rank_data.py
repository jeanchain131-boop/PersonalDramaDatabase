import json
import unittest
from unittest.mock import patch
from datetime import datetime, timedelta, timezone

import fetch_rank_data as ranks


class RankPartialUploadTests(unittest.TestCase):
    def sample_store(self) -> dict:
        return {
            "_meta": {"updated_at": "2026-04-27T00:00:00+00:00"},
            "missevan": {
                "ranks": {"new_daily": {"name": "新品日榜", "items": ["101"]}},
                "dramas": {"101": {"name": "猫耳剧", "view_count": 10}},
            },
            "manbo": {
                "ranks": {"hot": {"name": "热播榜", "items": [{"dramaId": "201"}]}},
                "dramas": {"201": {"name": "漫播剧", "view_count": 20}},
            },
        }

    def test_history_payloads_are_limited_to_selected_platform(self) -> None:
        payloads = ranks.build_rank_history_payloads(
            self.sample_store(),
            platforms=("missevan",),
            history_date="2026-04-27",
            generated_at="2026-04-27T00:00:00+00:00",
        )

        self.assertEqual(
            set(payloads),
            {
                "ranks:list:2026-04-27:missevan",
                "ranks:metrics:2026-04-27:missevan",
            },
        )

    def test_metric_payload_preserves_display_and_metadata_fields(self) -> None:
        store = self.sample_store()
        store["missevan"]["dramas"]["101"].update(
            {
                "cover": "https://example.test/cover.jpg",
                "maincvs": ["CV A", "CV B"],
                "catalogName": "广播剧",
                "payStatus": "付费",
                "createTime": "2026.04",
                "ignored": "not uploaded",
            }
        )

        payload = ranks._build_metric_payload(
            store,
            "missevan",
            "2026-04-27",
            "2026-04-27T00:00:00+00:00",
        )

        self.assertEqual(
            payload["dramas"]["101"],
            {
                "name": "猫耳剧",
                "view_count": 10,
                "cover": "https://example.test/cover.jpg",
                "maincvs": ["CV A", "CV B"],
                "catalogName": "广播剧",
                "payStatus": "付费",
                "createTime": "2026.04",
            },
        )

    def test_merge_uses_partial_and_falls_back_to_remote_full_store(self) -> None:
        merged = ranks.merge_rank_partials(
            {
                "missevan": {"ranks": {"new_daily": {"items": ["102"]}}, "dramas": {"102": {"name": "新猫耳"}}},
                "manbo": None,
            },
            fallback_store=self.sample_store(),
            generated_at="2026-04-27T01:00:00+00:00",
        )

        self.assertEqual(merged["missevan"]["dramas"]["102"]["name"], "新猫耳")
        self.assertEqual(merged["manbo"]["dramas"]["201"]["name"], "漫播剧")
        self.assertEqual(merged["_meta"]["updated_at"], "2026-04-27T01:00:00+00:00")

    def test_upload_history_sets_only_selected_platform_shards(self) -> None:
        commands = []

        def fake_upstash(command):
            commands.append(command)
            if command[0] == "EVAL":
                return json.dumps([])
            return "OK"

        with patch.object(ranks, "upstash_request", side_effect=fake_upstash):
            ranks.upload_rank_history(self.sample_store(), platforms=("manbo",))

        set_keys = [command[1] for command in commands if command[0] == "SET"]
        self.assertTrue(any(key.startswith("ranks:list:") and key.endswith(":manbo") for key in set_keys))
        self.assertTrue(any(key.startswith("ranks:metrics:") and key.endswith(":manbo") for key in set_keys))
        self.assertFalse(any(key.startswith("ranks:list:") and key.endswith(":missevan") for key in set_keys))
        self.assertFalse(any(key.startswith("ranks:metrics:") and key.endswith(":missevan") for key in set_keys))

    def test_upload_partials_sets_only_selected_platform_partial(self) -> None:
        commands = []

        def fake_upstash(command):
            commands.append(command)
            return "OK"

        with patch.object(ranks, "upstash_request", side_effect=fake_upstash):
            ranks.upload_rank_partials(self.sample_store(), platforms=("missevan",))

        set_keys = [command[1] for command in commands if command[0] == "SET"]
        self.assertEqual(set_keys, ["ranks:partial:missevan"])

    def test_atomic_queue_update_uses_eval_on_existing_queue_key(self) -> None:
        commands = []

        def fake_upstash(command):
            commands.append(command)
            return json.dumps({"missevan": ["101"], "manbo": ["201"]})

        with patch.object(ranks, "upstash_request", side_effect=fake_upstash):
            ranks.append_new_drama_ids_atomic(["101"], ["201"])

        self.assertEqual(commands[0][0], "EVAL")
        self.assertEqual(commands[0][3], ranks.QUEUE_KEY)

    def test_collect_manbo_danmaku_targets_uses_all_non_peak_ranks(self) -> None:
        store = {
            "manbo": {
                "ranks": {
                    "hot": {"items": [{"dramaId": "201"}, {"dramaId": "202"}]},
                    "box_office_paid": {"items": [{"dramaId": "202"}, {"drama_id": "203"}]},
                    "peak": {"items": [{"dramaId": "999"}]},
                }
            }
        }

        self.assertEqual(
            ranks.collect_manbo_danmaku_target_ids(store),
            {"201", "202", "203"},
        )

    def test_extract_ongoing_ids_supports_records_and_key_fallback(self) -> None:
        payload = {
            "records": {
                "101": {"dramaId": 1001, "updateType": "weekly"},
                "102": {"updateType": "daily"},
                "103": {"dramaId": "", "updateType": "daily"},
                "104": None,
            }
        }

        self.assertEqual(ranks.extract_ongoing_ids(payload), {"1001", "102", "103"})
        self.assertEqual(ranks.extract_ongoing_ids({}), set())
        self.assertEqual(ranks.extract_ongoing_ids({"records": []}), set())
        self.assertEqual(ranks.extract_ongoing_ids(None), set())

    def test_load_ongoing_drama_ids_reads_expected_key(self) -> None:
        loaded_keys = []

        def fake_load(key):
            loaded_keys.append(key)
            return {"records": {"101": {"dramaId": "101"}, "102": {}}}

        with patch.object(ranks, "_load_upstash_json", side_effect=fake_load):
            self.assertEqual(ranks.load_ongoing_drama_ids("missevan"), {"101", "102"})

        self.assertEqual(loaded_keys, ["ongoing:missevan"])

    def test_merge_rank_and_ongoing_ids_coerces_and_dedupes(self) -> None:
        self.assertEqual(
            ranks.merge_rank_and_ongoing_ids({101, "102"}, ["102", 103, None, ""]),
            {"101", "102", "103"},
        )

    def test_only_danmaku_mode_includes_existing_ongoing_ids(self) -> None:
        store = {
            "missevan": {
                "ranks": {"new_daily": {"items": ["101"]}},
                "dramas": {
                    "101": {"danmaku_uid_count": None},
                    "102": {"danmaku_uid_count": None},
                },
            },
            "manbo": {
                "ranks": {
                    "hot": {"items": [{"dramaId": "201"}]},
                    "peak": {"items": [{"dramaId": "999"}]},
                },
                "dramas": {
                    "201": {"danmaku_uid_count": None, "danmaku_paid_episode_count": None},
                    "202": {"danmaku_uid_count": None, "danmaku_paid_episode_count": None},
                },
            },
        }
        requested_missevan = []
        requested_manbo = []

        class FakeRequester:
            def request_json(self, url):
                requested_missevan.append(url.rsplit("=", 1)[-1])
                return {"info": {"episodes": {"episode": []}}}

        def fake_ongoing(platform):
            return {"missevan": {"102", "103"}, "manbo": {"202", "203"}}[platform]

        with (
            patch.object(ranks, "MissevanRequester", return_value=FakeRequester()),
            patch.object(ranks, "load_ongoing_drama_ids", side_effect=fake_ongoing),
            patch.object(ranks, "_fetch_missevan_danmaku", side_effect=lambda requester, episodes, entry: entry.update({"danmaku_uid_count": 0})),
            patch.object(ranks, "fetch_manbo_danmaku_details", side_effect=lambda ids, store, force: requested_manbo.extend(sorted(ids))),
            patch.object(ranks, "save_json"),
        ):
            ranks.only_danmaku_mode(store, force=True, do_missevan=True, do_manbo=True)

        self.assertEqual(sorted(requested_missevan), ["101", "102"])
        self.assertEqual(requested_manbo, ["201", "202"])

    def test_latest_rank_history_date_uses_latest_index_date(self) -> None:
        with patch.object(ranks, "load_rank_history_index", return_value={"dates": ["2026-04-27", "2026-04-29", "2026-04-28"]}):
            self.assertEqual(ranks.latest_rank_history_date(), "2026-04-29")

        with patch.object(ranks, "load_rank_history_index", return_value={"dates": []}):
            self.assertIsNone(ranks.latest_rank_history_date())

    def test_load_rank_metrics_reads_metrics_key(self) -> None:
        loaded_keys = []

        def fake_load(key):
            loaded_keys.append(key)
            return {
                "platform": "missevan",
                "dramas": {
                    "101": {
                        "fetched_at": "2026-04-29T00:00:00+00:00",
                        "cover": "https://example.test/cover.jpg",
                        "maincvs": ["CV A", "CV B"],
                        "catalogName": "广播剧",
                        "payStatus": "付费",
                        "createTime": "2026.04",
                    }
                },
            }

        with patch.object(ranks, "_load_upstash_json", side_effect=fake_load):
            self.assertEqual(
                ranks.load_rank_metrics("missevan", "2026-04-29"),
                {
                    "101": {
                        "fetched_at": "2026-04-29T00:00:00+00:00",
                        "cover": "https://example.test/cover.jpg",
                        "maincvs": ["CV A", "CV B"],
                        "catalogName": "广播剧",
                        "payStatus": "付费",
                        "createTime": "2026.04",
                    }
                },
            )

        self.assertEqual(loaded_keys, ["ranks:metrics:2026-04-29:missevan"])

    def test_load_remote_platform_store_uses_partial_ranks_and_latest_metrics_dramas(self) -> None:
        partial = {
            "ranks": {"hot": {"items": ["101"]}},
            "dramas": {"101": {"fetched_at": "old-local"}},
        }
        metrics = {
            "101": {
                "fetched_at": "metric-time",
                "view_count": 100,
                "cover": "https://example.test/cover.jpg",
                "maincvs": ["CV A", "CV B"],
            }
        }

        with (
            patch.object(ranks, "latest_rank_history_date", return_value="2026-04-29"),
            patch.object(ranks, "load_rank_partial", return_value=partial),
            patch.object(ranks, "load_rank_metrics", return_value=metrics),
        ):
            store = ranks.load_remote_platform_store_for_fetch("missevan")

        self.assertEqual(store["ranks"], partial["ranks"])
        self.assertEqual(store["dramas"], metrics)

    def test_load_initial_rank_store_prefers_remote_without_local_read(self) -> None:
        remote_platforms = {
            "missevan": {"ranks": {"new_daily": {"items": ["101"]}}, "dramas": {"101": {"fetched_at": "remote"}}},
            "manbo": {"ranks": {}, "dramas": {}},
        }

        with (
            patch.object(ranks, "load_remote_platform_store_for_fetch", side_effect=lambda platform: remote_platforms[platform]),
            patch.object(ranks, "load_json", side_effect=AssertionError("local should not be read")),
        ):
            store = ranks.load_initial_rank_store()

        self.assertEqual(store["missevan"]["dramas"]["101"]["fetched_at"], "remote")

    def test_load_initial_rank_store_falls_back_to_local_then_empty(self) -> None:
        local_store = self.sample_store()

        with (
            patch.object(ranks, "load_remote_platform_store_for_fetch", side_effect=RuntimeError("remote unavailable")),
            patch.object(ranks, "load_json", return_value=local_store),
        ):
            self.assertEqual(ranks.load_initial_rank_store(), local_store)

        with (
            patch.object(ranks, "load_remote_platform_store_for_fetch", side_effect=RuntimeError("remote unavailable")),
            patch.object(ranks, "load_json", return_value=None),
        ):
            store = ranks.load_initial_rank_store()
            self.assertEqual(store["missevan"]["dramas"], {})
            self.assertEqual(store["manbo"]["dramas"], {})

    def test_select_stale_ids_uses_remote_metric_fetched_at(self) -> None:
        fresh = datetime.now(timezone.utc).isoformat()
        stale = (datetime.now(timezone.utc) - timedelta(hours=13)).isoformat()
        dramas = {
            "101": {"fetched_at": fresh},
            "102": {"fetched_at": stale},
        }

        to_update, skipped = ranks.select_stale_ids({"101", "102", "103"}, dramas, force=False)

        self.assertEqual(to_update, {"102", "103"})
        self.assertEqual(skipped, 1)

    def test_select_manbo_danmaku_backfill_ids_excludes_detail_updates(self) -> None:
        self.assertEqual(
            ranks.select_manbo_danmaku_backfill_ids({"201", "202", "203"}, {"202"}),
            {"201", "203"},
        )

    def test_pay_status_marks_paid_member_dramas_as_member(self) -> None:
        self.assertEqual(ranks.pay_status_from_metadata({"needpay": True, "is_member": True}), "会员")
        self.assertEqual(ranks.pay_status_from_metadata({"needpay": True, "vipFree": 1}), "会员")
        self.assertEqual(ranks.pay_status_from_metadata({"needpay": True, "vipFree": 0}), "付费")
        self.assertEqual(ranks.pay_status_from_metadata({"needpay": False, "vipFree": 1}), "免费")

    def test_lookup_cvs_uses_member_fields_for_pay_status(self) -> None:
        store = {
            "missevan": {"dramas": {"101": {}}},
            "manbo": {"dramas": {"201": {}}},
        }

        def fake_load(key):
            if key == "missevan:info:v1":
                return {
                    "101": {
                        "cvnames": {"1": "CV A"},
                        "maincvs": [1],
                        "needpay": True,
                        "is_member": True,
                    }
                }
            if key == "manbo:info:v1":
                return {
                    "records": [
                        {
                            "dramaId": "201",
                            "mainCvNames": ["CV B"],
                            "needpay": True,
                            "vipFree": 1,
                        }
                    ]
                }
            return None

        with patch.object(ranks, "_load_upstash_json", side_effect=fake_load):
            ranks.lookup_cvs(store)

        self.assertEqual(store["missevan"]["dramas"]["101"]["payStatus"], "会员")
        self.assertEqual(store["manbo"]["dramas"]["201"]["payStatus"], "会员")

    def test_fetch_manbo_drama_details_refreshes_danmaku_only_for_updated_eligible_ids(self) -> None:
        store = {"manbo": {"dramas": {}}}
        fetched_details = []
        fetched_danmaku = []

        def fake_fetch_one(drama_id, entry):
            fetched_details.append(drama_id)
            entry["name"] = f"drama-{drama_id}"

        def fake_danmaku(drama_id):
            fetched_danmaku.append(drama_id)
            return drama_id, 7, 2

        with (
            patch.object(ranks, "_fetch_one_manbo", side_effect=fake_fetch_one),
            patch.object(ranks, "fetch_one_manbo_danmaku_count", side_effect=fake_danmaku),
            patch.object(ranks, "save_json"),
        ):
            ranks.fetch_manbo_drama_details(
                {"201", "202"},
                store,
                skip_danmaku=False,
                danmaku_ids={"202", "203"},
            )

        self.assertEqual(sorted(fetched_details), ["201", "202"])
        self.assertEqual(fetched_danmaku, ["202"])
        self.assertNotIn("danmaku_uid_count", store["manbo"]["dramas"]["201"])
        self.assertEqual(store["manbo"]["dramas"]["202"]["danmaku_uid_count"], 7)
        self.assertEqual(store["manbo"]["dramas"]["202"]["danmaku_paid_episode_count"], 2)

    def test_fetch_manbo_drama_details_skip_danmaku_preserves_existing_fields(self) -> None:
        store = {
            "manbo": {
                "dramas": {
                    "201": {
                        "danmaku_uid_count": 9,
                        "danmaku_paid_episode_count": 3,
                    }
                }
            }
        }

        def fake_fetch_one(drama_id, entry):
            entry["name"] = "updated"

        with (
            patch.object(ranks, "_fetch_one_manbo", side_effect=fake_fetch_one),
            patch.object(ranks, "fetch_one_manbo_danmaku_count", side_effect=AssertionError("should not fetch danmaku")),
            patch.object(ranks, "save_json"),
        ):
            ranks.fetch_manbo_drama_details(
                {"201"},
                store,
                skip_danmaku=True,
                danmaku_ids={"201"},
            )

        self.assertEqual(store["manbo"]["dramas"]["201"]["danmaku_uid_count"], 9)
        self.assertEqual(store["manbo"]["dramas"]["201"]["danmaku_paid_episode_count"], 3)


if __name__ == "__main__":
    unittest.main()
