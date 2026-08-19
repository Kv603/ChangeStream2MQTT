import os
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import changestream2mqtt as app


class SlackChangeTests(unittest.TestCase):
    def setUp(self):
        self.database = MagicMock()
        self.database.__getitem__.return_value.aggregate.return_value = [
            {"slack_id": "U123"}
        ]

    def test_rejection_message_includes_mention_and_expiry(self):
        now_ms = 1_800_000_000_000
        document = {
            "uid": "CARD", "validity": "Expired", "where": "front-door",
            "holder": "Chris", "timeOf": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "expiry": now_ms - (4 * 24 * 60 * 60 * 1000),
        }
        with patch.object(app, "push_to_slack", new=AsyncMock(return_value=True)) as push:
            app.handle_slack_change(
                self.database, "rejections", "insert", document,
                now_ms=now_ms,
            )
        self.assertIn("rejected Expired Chris <@U123>", push.await_args.args[0])
        self.assertIn("(expired", push.await_args.args[0])

    def test_active_member_uses_whole_future_duration(self):
        now_ms = 1_800_000_000_000
        self.database.slack_users.find_one.return_value = {"slack_id": "U456"}
        document = {
            "_id": "member", "status": "activeMember", "cardID": "CARD",
            "firstname": "Ada", "lastname": "Lovelace",
            "expirationTime": now_ms + (22 * 24 * 60 * 60 * 1000),
        }
        with patch.object(app, "push_to_slack", new=AsyncMock(return_value=True)) as push:
            app.handle_slack_change(self.database, "members", "update", document,
                                    now_ms=now_ms)
        self.assertIn("expires in 3 weeks", push.await_args.args[0])
        self.assertIn("<@U456>", push.await_args.args[0])

    def test_missing_slack_settings_skips_request(self):
        with patch.dict(os.environ, {}, clear=True), \
                self.assertLogs(app.logger, level="WARNING") as logs:
            result = app.run_async(app.push_to_slack("message", ":robot_face:"))
        self.assertFalse(result)
        self.assertIn("SLACK_WEBHOOK_URL, SLACK_CHANNEL", logs.output[0])

    def test_duration_and_eastern_time_helpers(self):
        self.assertEqual(app.whole_duration(345_678_900), "4 days")
        rendered = app.convert_to_eastern(datetime(2026, 8, 18, 23, 12,
                                                   tzinfo=timezone.utc))
        self.assertEqual(rendered, "2026-08-18 07:12:00 PM EDT")


if __name__ == "__main__":
    unittest.main()
