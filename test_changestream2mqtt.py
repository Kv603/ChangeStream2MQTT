import asyncio
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import changestream2mqtt as app
from bson import ObjectId
from collection_handlers import NotificationDependencies
from collection_handlers import checkins, rejections
from collection_handlers.common import (SLACK_USER_CACHE_FILENAME,
                                        _reset_slack_user_cache,
                                        slack_user_for_card)


def dependencies(now_ms=1_800_000_000_000):
    """Return synchronous test doubles for handler transports."""
    push = AsyncMock(return_value=True)
    get = AsyncMock(return_value=None)
    run = MagicMock(side_effect=lambda coroutine: coroutine.close())
    return NotificationDependencies(push, get, run, lambda: now_ms), push, get, run


class CollectionHandlerTests(unittest.TestCase):
    def setUp(self):
        self.cache_environment = patch.dict(os.environ, {"CACHEDIR": ""})
        self.cache_environment.start()
        self.addCleanup(self.cache_environment.stop)
        _reset_slack_user_cache()
        self.addCleanup(_reset_slack_user_cache)
        self.database = MagicMock()
        self.member_id = ObjectId("507f1f77bcf86cd799439011")
        self.database.cards.find_one.return_value = {"member_id": self.member_id}
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

    def test_repeated_identity_uses_ram_cache_without_requerying_database(self):
        identity = {
            "holder": "Kevin Kadow", "name": "Kevin", "uid": "1a78fca0",
        }
        first_mention = slack_user_for_card(self.database, identity)
        second_mention = slack_user_for_card(self.database, identity)

        self.assertEqual(first_mention, "<@U123>")
        self.assertEqual(second_mention, "<@U123>")
        self.database.cards.find_one.assert_called_once_with(
            {"uid": {"$in": ["1a78fca0", "1A78FCA0"]}}, {"member_id": 1}
        )
        self.database.slack_users.find_one.assert_called_once_with(
            {"member_id": {"$in": [self.member_id, str(self.member_id)]}},
            {"slack_id": 1},
        )
        self.database.__getitem__.assert_not_called()
        self.database.members.find_one.assert_not_called()

    def test_different_present_field_combinations_use_distinct_cache_keys(self):
        slack_user_for_card(self.database,
                            {"holder": "Kevin Kadow", "uid": "CARD"})
        slack_user_for_card(self.database, {"name": "Kevin", "uid": "CARD"})
        slack_user_for_card(self.database, {"uid": "CARD"})

        self.assertEqual(self.database.cards.find_one.call_count, 3)
        self.assertEqual(self.database.slack_users.find_one.call_count, 3)

    def test_cached_identity_is_persisted_and_loaded_from_cachedir(self):
        identity = {"holder": "Kevin Kadow", "uid": "1A78FCA0"}
        cache_dir = Path(__file__).resolve().parent / ".test_slack_user_cache"
        cache_file = cache_dir / SLACK_USER_CACHE_FILENAME
        cache_dir.mkdir(exist_ok=True)
        cache_file.unlink(missing_ok=True)
        try:
            with patch.dict(os.environ, {"CACHEDIR": str(cache_dir)}):
                _reset_slack_user_cache()
                self.assertEqual(slack_user_for_card(self.database, identity),
                                 "<@U123>")
                self.assertTrue(cache_file.is_file())

                _reset_slack_user_cache()
                fresh_database = MagicMock()
                self.assertEqual(slack_user_for_card(fresh_database, identity),
                                 "<@U123>")
                fresh_database.cards.find_one.assert_not_called()
                fresh_database.slack_users.find_one.assert_not_called()
        finally:
            _reset_slack_user_cache()
            cache_file.unlink(missing_ok=True)
            cache_dir.rmdir()

    def test_card_lookup_returns_empty_when_card_or_slack_mapping_is_missing(self):
        self.database.cards.find_one.return_value = None
        self.assertEqual(slack_user_for_card(self.database, "CARD"), "")
        self.database.slack_users.find_one.assert_not_called()

        self.database.cards.find_one.return_value = {"member_id": self.member_id}
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

    def test_total_slack_timeout_is_logged_and_does_not_escape(self):
        class TimeoutRequest:
            async def __aenter__(self):
                raise asyncio.TimeoutError("total request timeout")

            async def __aexit__(self, *_):
                return False

        class TimeoutSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                return False

            def post(self, *_args, **_kwargs):
                return TimeoutRequest()

        settings = {
            "SLACK_WEBHOOK_URL": "https://hooks.slack.test/example",
            "SLACK_CHANNEL": "#test",
        }
        with patch.dict(os.environ, settings), \
                patch.object(app.aiohttp, "ClientSession",
                             return_value=TimeoutSession()), \
                self.assertLogs(app.logger, level="ERROR") as logs:
            result = app.run_async(app.push_to_slack("message", ":robot_face:"))

        self.assertFalse(result)
        self.assertIn("total request timeout", logs.output[0])

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

    def test_registered_handler_runs_after_one_generic_publish(self):
        order = []
        self.mqtt.publish.side_effect = lambda *_args: order.append("mqtt") or (0,)
        handler = MagicMock(side_effect=lambda *_args: order.append("handler"))
        with patch.dict(app.COLLECTION_HANDLERS, {"checkins": handler}, clear=True):
            app.process_mongo_update(self.change(), self.database, self.mqtt)
        handler.assert_called_once()
        self.mqtt.publish.assert_called_once()
        self.assertEqual(self.mqtt.publish.call_args.args[0], "checkins/insert")
        self.assertEqual(order, ["mqtt", "handler"])

    def test_unknown_collection_publishes_before_generic_slack_handling(self):
        order = []
        self.mqtt.publish.side_effect = lambda *_args: order.append("mqtt") or (0,)
        with patch.object(
            app, "handle_slack_change",
            side_effect=lambda *_args: order.append("slack"),
        ) as slack:
            app.process_mongo_update(self.change("widgets"), self.database, self.mqtt)
        slack.assert_called_once()
        self.mqtt.publish.assert_called_once()
        self.assertEqual(self.mqtt.publish.call_args.args[0], "widgets/insert")
        self.assertEqual(order, ["mqtt", "slack"])

    def test_null_update_lookup_publishes_to_mqtt_and_skips_slack(self):
        change = {
            "ns": {"coll": "members"},
            "operationType": "update",
            "fullDocument": None,
        }
        with patch.object(app, "handle_slack_change") as slack:
            app.process_mongo_update(change, self.database, self.mqtt)

        self.mqtt.publish.assert_called_once()
        self.assertEqual(self.mqtt.publish.call_args.args[0], "members/update")
        self.assertIn('"document": null', self.mqtt.publish.call_args.args[1])
        slack.assert_not_called()

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
