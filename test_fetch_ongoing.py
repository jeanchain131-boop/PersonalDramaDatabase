import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import fetch_ongoing as ongoing


class FetchOngoingTests(unittest.TestCase):
    def test_missevan_sound_html_parses_thresholds_and_collector_stops_at_20(self) -> None:
        def block(sound_id: int, views: int, comments: int) -> str:
            return f'''
            <div class="vw-frontsound-container fc-hoverheight-container floatleft">
              <a target="_player" href="/sound/{sound_id}" title="title {sound_id}">
                <div class="vw-frontsound-viewcount floatleft">{views}</div>
                <div class="vw-frontsound-commentcount floatleft">{comments}</div>
              </a>
            </div>
            '''

        page_one = (
            block(1, 99, 19)
            + block(2, 100, 0)
            + block(3, 0, 20)
            + "".join(block(i, 101, 0) for i in range(4, 30))
        )
        fetched_pages: list[int] = []

        def fetch_html(page: int) -> str:
            fetched_pages.append(page)
            return page_one if page == 1 else ""

        self.assertEqual(
            ongoing.collect_missevan_daily_sound_ids(fetch_html),
            [str(i) for i in range(2, 22)],
        )
        self.assertEqual(fetched_pages, [1])

    def test_daily_records_are_deduped_and_weekly_records_win(self) -> None:
        records = ongoing.merge_records(
            weekly=[
                ongoing.make_record("42", "weekly"),
                ongoing.make_record("99", "weekly"),
            ],
            daily=[
                ongoing.make_record("42", "daily"),
                ongoing.make_record("43", "daily"),
                ongoing.make_record("43", "daily"),
            ],
        )

        self.assertEqual(records["42"]["updateType"], "weekly")
        self.assertEqual(records["43"]["updateType"], "daily")
        self.assertNotIn("weekday", records["42"])
        self.assertNotIn("weekday", records["43"])
        self.assertEqual(list(records), ["42", "99", "43"])

    def test_missevan_app_timeline_records_keep_only_paid_dramas(self) -> None:
        payload = {
            "success": True,
            "info": [
                {"date_week": "一", "dramas": [{"id": 101, "pay_type": 2}, {"id": 102, "pay_type": 0}]},
                {"date_week": "二", "dramas": [{"id": 201, "pay_type": 2}, {"id": 202}]},
            ],
        }

        records = ongoing.parse_missevan_timeline_weekly_records(payload)

        self.assertEqual(
            records,
            [
                ongoing.make_record("101", "weekly"),
                ongoing.make_record("201", "weekly"),
            ],
        )

    def test_missevan_weekly_prefers_app_timeline_and_falls_back_to_summerdrama(self) -> None:
        class FakeRequester:
            def __init__(self) -> None:
                self.urls: list[str] = []

            def request_json(self, url: str) -> dict:
                self.urls.append(url)
                return {"info": [[{"id": 9}], [{"id": 10}]]}

        timeline_calls = 0

        def failing_timeline() -> dict:
            nonlocal timeline_calls
            timeline_calls += 1
            raise RuntimeError("timeline auth failed")

        records = ongoing.fetch_missevan_weekly_records(FakeRequester(), fetch_timeline=failing_timeline)

        self.assertEqual(timeline_calls, 1)
        self.assertEqual([record["dramaId"] for record in records], ["9", "10"])
        self.assertTrue(all(record["updateType"] == "weekly" for record in records))
        self.assertTrue(all("weekday" not in record for record in records))

    def test_manbo_timestamp_list_uses_previous_7_beijing_midnights(self) -> None:
        now = datetime(2026, 4, 28, 1, 0, tzinfo=timezone.utc)
        timestamps = ongoing.previous_7_beijing_midnight_timestamps(now=now)
        dates = [
            datetime.fromtimestamp(ts / 1000, tz=ongoing.BEIJING_TZ).strftime("%Y-%m-%d %H:%M")
            for ts in timestamps
        ]

        self.assertEqual(
            dates,
            [
                "2026-04-21 00:00",
                "2026-04-22 00:00",
                "2026-04-23 00:00",
                "2026-04-24 00:00",
                "2026-04-25 00:00",
                "2026-04-26 00:00",
                "2026-04-27 00:00",
            ],
        )
        self.assertTrue(all(ts > 10_000_000_000 for ts in timestamps))

    def test_manbo_item_filters_and_builds_weekly_and_daily_records(self) -> None:
        def item(
            item_id: str,
            *,
            drama_id: str | None = None,
            category: int = 1,
            title: str = "正剧第1集",
            update_time: str = "10:00",
            labels: list[str] | None = None,
            price: int = 10,
            member_price: int = 10,
            vip_free: int = 0,
        ) -> dict:
            return {
                "id": item_id,
                "dateNodeFormat": "三",
                "workUpdateTimeFormat": update_time,
                "updateSetTitle": title,
                "radioDramaResp": {
                    "radioDramaIdStr": drama_id or f"drama-{item_id}",
                    "category": category,
                    "price": price,
                    "memberPrice": member_price,
                    "vipFree": vip_free,
                    "categoryLabels": [{"name": label} for label in (labels or ["正剧"])],
                },
            }

        records = ongoing.collect_manbo_records_from_items(
            [
                item("item-weekly-paid", drama_id="weekly-paid"),
                item("item-daily-member", drama_id="daily-member", category=5, price=0, member_price=0, vip_free=1),
                item("free", price=0, member_price=0, vip_free=0),
                item("label", labels=["全一期"]),
                item("early", update_time="09:58"),
                item("late", update_time="20:02"),
                item("bonus", title="福利小剧场"),
            ]
        )

        self.assertEqual(set(records), {"weekly-paid", "daily-member"})
        self.assertEqual(records["weekly-paid"]["updateType"], "weekly")
        self.assertEqual(records["daily-member"]["updateType"], "daily")
        self.assertNotIn("weekday", records["weekly-paid"])
        self.assertNotIn("weekday", records["daily-member"])

    def test_upload_uses_ongoing_keys_and_dry_run_writes_local_json(self) -> None:
        commands: list[list[object]] = []
        payload = ongoing.build_payload(
            "missevan",
            {"101": ongoing.make_record("101", "daily")},
            generated_at="2026-04-28T00:00:00+00:00",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            ongoing.upload_payload("missevan", payload, upstash=lambda command: commands.append(command) or "OK")
            dry_path = ongoing.upload_payload(
                "manbo",
                payload,
                upstash=lambda command: commands.append(command) or "OK",
                dry_run=True,
                dry_run_dir=Path(temp_dir),
            )

            self.assertEqual(commands[0][0], "SET")
            self.assertEqual(commands[0][1], "ongoing:missevan")
            self.assertEqual(json.loads(commands[0][2])["records"]["101"]["updateType"], "daily")
            self.assertNotIn("weekday", json.loads(commands[0][2])["records"]["101"])
            self.assertEqual(len(commands), 1)
            self.assertEqual(dry_path, Path(temp_dir) / "ongoing-manbo.json")
            self.assertEqual(json.loads(dry_path.read_text(encoding="utf-8")), payload)


if __name__ == "__main__":
    unittest.main()
