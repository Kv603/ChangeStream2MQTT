import os
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import changestream2mqtt as app
from bson import ObjectId
from collection_handlers import NotificationDependencies
from collection_handlers import checkins, rejections
from collection_handlers.common import slack_user_for_card


def dependencies(now_ms=1_800_000_000_000):
    """Return synchronous test doubles for handler transports."""
    push = AsyncMock(return_value=True)
    get = AsyncMock(return_value=None)
    run = MagicMock(side_effect=lambda coroutine: coroutine.close())
    return NotificationDependencies(push, get, run, lambda: now_ms), push, get, run


class CollectionHandlerTests(unittest.TestCase):
    def setUp(self):
        self.database = MagicMock()
        self.member_id = ObjectId("507f1f77bcf86cd799439011")
        self.database.members.find_one.return_value = {"_id": self.member_id}
        self.database.slack_users.find_one.return_value = {"slack_id": "U123"}

    def test_rejection_message_includes_mention_and_expiry(self):
        now_ms = 1_800_000_000_000
        document = {
            "uid": "CARD", "validity": "Expired", "where": "front-door",
            "holder": "Chris", "timeOf": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "expiry": now_ms - (4 * 24 * 60 * 60 * 1000),
        }
        deps, push, _, run = dependencies(now_ms)
        rejections.handle_change(self.database, "insert", document, deps)
        self.assertIn("rejected Expired Chris <@U123>", push.call_args.args[0])
        self.assertIn("(expired 4 days ago)", push.call_args.args[0])
        self.assertEqual(push.call_args.args[1], ":octagonal_sign:")
        run.assert_called_once()

    def test_checkin_message_mention_emoji_and_legacy_get(self):
        document = {
            "_id": ObjectId("6a877dcca7507a33669a8051"),
            "uid": "1A78FCA0", "validity": "activeMember",
            "where": "doorboto32", "holder": "Kevin Kadow",
            "timeOf": datetime(2026, 8, 20, 22, 21, 0, 808000,
                               tzinfo=timezone.utc),
        }
        deps, push, notify_get, run = dependencies()
        with patch.dict(os.environ, {"SLACK_BOT_EMOJI_CHECKINS": ":key:"}):
            checkins.handle_change(self.database, "insert", document, deps)
        self.assertIn("granted access to activeMember Kevin Kadow <@U123>",
                      push.call_args.args[0])
        self.assertEqual(push.call_args.args[1], ":key:")
        notify_get.assert_called_once_with()
        self.assertEqual(run.call_count, 2)

    def test_card_lookup_uses_event_uid_without_requerying_checkins(self):
        mention = slack_user_for_card(self.database, "1A78FCA0")

        self.assertEqual(mention, "<@U123>")
        self.database.members.find_one.assert_called_once_with(
            {"cardID": {"$in": ["1A78FCA0", "1a78fca0"]}}, {"_id": 1}
        )
        self.database.slack_users.find_one.assert_called_once_with(
            {"member_id": {"$in": [self.member_id, str(self.member_id)]}},
            {"slack_id": 1},
        )
        self.database.__getitem__.assert_not_called()

    def test_card_lookup_returns_empty_when_member_or_slack_mapping_is_missing(self):
        self.database.members.find_one.return_value = None
        self.assertEqual(slack_user_for_card(self.database, "CARD"), "")
        self.database.slack_users.find_one.assert_not_called()

        self.database.members.find_one.return_value = {"_id": self.member_id}
        self.database.slack_users.find_one.return_value = None
        self.assertEqual(slack_user_for_card(self.database, "CARD"), "")

    def test_non_insert_operations_have_no_side_effects(self):
        for handler in (checkins.handle_change, rejections.handle_change):
            deps, push, notify_get, run = dependencies()
            handler(self.database, "update", {"holder": "Chris"}, deps)
            push.assert_not_called()
            notify_get.assert_not_called()
            run.assert_not_called()


class SlackChangeTests(unittest.TestCase):
    def setUp(self):
        self.database = MagicMock()

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


class DispatcherTests(unittest.TestCase):
    def setUp(self):
        self.database = MagicMock()
        self.mqtt = MagicMock()
        self.mqtt.publish.return_value = (0,)

    def change(self, collection="checkins", operation="insert"):
        return {"ns": {"coll": collection}, "operationType": operation,
                "fullDocument": {"uid": "CARD", "holder": None}}

    def test_registered_handler_runs_before_one_generic_publish(self):
        handler = MagicMock()
        with patch.dict(app.COLLECTION_HANDLERS, {"checkins": handler}, clear=True):
            app.process_mongo_update(self.change(), self.database, self.mqtt)
        handler.assert_called_once()
        self.mqtt.publish.assert_called_once()
        self.assertEqual(self.mqtt.publish.call_args.args[0], "checkins/insert")

    def test_unknown_collection_falls_back_to_one_generic_publish(self):
        with patch.object(app, "handle_slack_change") as slack:
            app.process_mongo_update(self.change("widgets"), self.database, self.mqtt)
        slack.assert_called_once()
        self.mqtt.publish.assert_called_once()
        self.assertEqual(self.mqtt.publish.call_args.args[0], "widgets/insert")

    def test_unsupported_operation_still_publishes_exactly_once(self):
        with patch.object(checkins, "handle_change") as handler:
            # Registry values are captured at import time, so replace the entry.
            with patch.dict(app.COLLECTION_HANDLERS,
                            {"checkins": handler}, clear=True):
                app.process_mongo_update(self.change(operation="delete"),
                                         self.database, self.mqtt)
        handler.assert_called_once()
        self.mqtt.publish.assert_called_once()

    def test_delete_without_full_document_is_dispatched_and_published(self):
        handler = MagicMock()
        change = {"ns": {"coll": "checkins"}, "operationType": "delete",
                  "documentKey": {"_id": "gone"}}
        with patch.dict(app.COLLECTION_HANDLERS,
                        {"checkins": handler}, clear=True):
            app.process_mongo_update(change, self.database, self.mqtt)
        self.assertEqual(handler.call_args.args[2], {"_id": "gone"})
        self.mqtt.publish.assert_called_once()


if __name__ == "__main__":
    unittest.main()
