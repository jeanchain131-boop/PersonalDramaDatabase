import json
import unittest
from unittest.mock import patch

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


if __name__ == "__main__":
    unittest.main()
