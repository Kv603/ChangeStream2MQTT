import json
import unittest
from datetime import datetime, timedelta, timezone
from itertools import count
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import paho.mqtt.client as mqtt
from pymongo.monitoring import CommandFailedEvent, CommandStartedEvent, CommandSucceededEvent

import service_status as status


HOUR = status.HOUR
ADDRESS = ("mongo", 27017)
CONNECTED = SimpleNamespace(is_failure=False)


class Clock:
    def __init__(self):
        self.wall = datetime(2026, 11, 24, 15, 37, tzinfo=timezone.utc).timestamp()
        self.steady = 100

    def advance(self, seconds):
        self.wall += seconds
        self.steady += seconds


class StatusTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.client = Mock()
        self.client.is_connected.return_value = False
        self.client.publish.side_effect = self.publish
        self.ids = count(1)
        self.sent = []
        self.change_rc = mqtt.MQTT_ERR_SUCCESS
        self.ack_status = True
        self.ack_changes = False
        self.settings = {
            "MQTT_HOST": "broker", "MQTT_USER": "bridge",
            "MQTT_PW": "private", "LOGHOST": "syslog",
        }

        with (
            patch.object(status.socket, "gethostname", return_value="bridge-1"),
            patch.object(status.subprocess, "run", return_value=SimpleNamespace(
                stdout="Linux bridge-1 test\n",
            )) as uname,
        ):
            self.reporter = status.StatusReporter(
                self.client, self.settings,
                wall_clock=lambda: self.clock.wall, monotonic=lambda: self.clock.steady,
            )
        self.uname = uname

    def publish(self, topic, payload, qos, *, retain):
        info = mqtt.MQTTMessageInfo(next(self.ids))
        info.rc = mqtt.MQTT_ERR_SUCCESS if retain else self.change_rc
        self.sent.append((info.mid, topic, payload, qos, retain))
        if (retain and self.ack_status) or (not retain and self.ack_changes):
            self.ack(info.mid)
        return info

    def ack(self, mid):
        self.client.on_publish(self.client, None, mid, CONNECTED, None)

    def connect(self):
        self.client.is_connected.return_value = True
        self.client.on_connect(self.client, None, None, CONNECTED, None)
        self.pump()

    def disconnect(self):
        self.client.is_connected.return_value = False
        self.client.on_disconnect(self.client, None, None, None, None)
        self.pump()

    def pump(self):
        self.reporter._step(max_wait=0)
        while not self.reporter._events.empty():
            self.reporter._step(max_wait=0)

    def messages(self, topic, event=None):
        messages = [json.loads(payload) for _, sent_topic, payload, qos, retain in self.sent
                    if sent_topic == topic and qos == 1 and retain]
        return messages if event is None else [msg for msg in messages if msg["event"] == event]

    def send_change(self, rc=mqtt.MQTT_ERR_SUCCESS):
        self.change_rc = rc
        return self.reporter.publish("widgets/update", "change", 2)

    def test_startup_and_retained_last_will(self):
        topic, payload = self.client.will_set.call_args.args
        self.assertEqual(topic, status.MQTT_STATUS_TOPIC)
        self.assertEqual(self.client.will_set.call_args.kwargs, {"qos": 1, "retain": True})
        self.assertEqual(json.loads(payload), {
            "timestamp": "2026-11-24T15:37:00.000Z", "timestamp_kind": "will_configured",
            "hostname": "bridge-1", "event": "connection_lost", "status": "disconnected",
            "message": "connection lost",
        })
        self.client.on_connect(
            self.client, None, None, SimpleNamespace(is_failure=True), None,
        )
        self.pump()
        self.client.publish.assert_not_called()

        self.connect()
        startup, = self.messages(status.STATUS_TOPIC)
        self.assertEqual(startup["event"], "startup")
        self.assertEqual(startup["status"], "started")
        self.assertEqual(startup["uname"], "Linux bridge-1 test")
        self.assertEqual(startup["hostname"], "bridge-1")
        self.assertEqual(startup["environment"], {
            "MQTT_HOST": "broker", "MQTT_USER": "bridge", "LOGHOST": "syslog",
        })
        self.assertEqual(self.uname.call_args.args, (["uname", "-a"],))
        self.assertGreater(self.uname.call_args.kwargs["timeout"], 0)
        for path_key in ("executable", "entrypoint"):
            path = Path(startup[path_key]["path"])
            self.assertTrue(path.is_absolute())
            self.assertEqual(path.name, startup[path_key]["name"])

        self.disconnect()
        self.connect()
        self.assertEqual(len(self.messages(status.STATUS_TOPIC)), 1)
        self.assertEqual([msg["event"] for msg in self.messages(status.MQTT_STATUS_TOPIC)],
                         ["connection_established", "connection_resumed"])

    def test_missing_uname_and_optional_loghost(self):
        with (
            patch.object(status.subprocess, "run", side_effect=FileNotFoundError),
            patch.object(status.platform, "uname", return_value=("Windows", "bridge")),
        ):
            details = status._startup_details({"MQTT_HOST": "broker"})
        self.assertEqual(details["uname"], "Windows bridge")
        self.assertEqual(details["environment"], {"MQTT_HOST": "broker", "MQTT_USER": None})

    def test_reconnect_keeps_status_order_and_latest_mongodb_state(self):
        self.ack_status = False
        self.connect()
        self.reporter.mongodb_opened()
        self.pump()
        old_mqtt = next(mid for mid, topic, *_ in self.sent if topic == status.MQTT_STATUS_TOPIC)
        old_mongo = next(mid for mid, topic, *_ in self.sent if topic == status.MONGODB_STATUS_TOPIC)

        self.disconnect()
        self.reporter.mongodb_lost("get_more_failed")
        self.reporter.mongodb_opened()
        self.reporter.mongodb_lost("watch_error")
        self.clock.advance(23 * 60)
        self.pump()
        self.connect()
        self.assertEqual(len(self.messages(status.MONGODB_STATUS_TOPIC)), 1)
        self.assertEqual(len(self.messages(status.MQTT_STATUS_TOPIC)), 1)

        self.ack(old_mqtt)
        self.ack(old_mongo)
        self.pump()
        mqtt_updates = self.messages(status.MQTT_STATUS_TOPIC)
        mongo_updates = self.messages(status.MONGODB_STATUS_TOPIC)
        self.assertEqual([msg["event"] for msg in mqtt_updates],
                         ["connection_established", "connection_resumed"])
        self.assertEqual([msg["event"] for msg in mongo_updates],
                         ["connection_established", "connection_lost"])
        self.assertEqual(mongo_updates[-1]["reason"], "watch_error")

    def test_pymongo_stream_commands_observe_loss_and_transparent_resume(self):
        self.connect()
        listener = status.MongoStreamListener(self.reporter, "makerauth")
        watch = {"aggregate": 1, "pipeline": [{"$changeStream": {}}]}

        def start(command, request):
            listener.started(CommandStartedEvent(
                command, "makerauth", request, ADDRESS, None,
            ))

        def success(name, request, cursor):
            listener.succeeded(CommandSucceededEvent(
                timedelta(), {"cursor": {"id": cursor, "nextBatch": []}},
                name, request, ADDRESS, None,
            ))

        def fail_get_more(request):
            listener.failed(CommandFailedEvent(
                timedelta(), {"code": 43}, "getMore", request, ADDRESS, None,
            ))

        start({"aggregate": "regular_collection", "pipeline": []}, 1)
        success("aggregate", 1, 11)
        start(watch, 2)
        success("aggregate", 2, 22)
        start({"getMore": 11, "collection": "regular_collection"}, 3)
        fail_get_more(3)
        start({"getMore": 22, "collection": "$cmd.aggregate"}, 4)
        success("getMore", 4, 22)
        self.pump()
        self.assertEqual([msg["event"] for msg in self.messages(status.MONGODB_STATUS_TOPIC)],
                         ["connection_established"])

        start({"getMore": 22, "collection": "$cmd.aggregate"}, 5)
        fail_get_more(5)
        start(watch, 6)
        success("aggregate", 6, 33)
        start(watch, 7)
        success("aggregate", 7, 44)
        self.pump()
        updates = self.messages(status.MONGODB_STATUS_TOPIC)
        self.assertEqual([msg["event"] for msg in updates], [
            "connection_established", "connection_lost", "connection_resumed",
            "connection_lost", "connection_resumed",
        ])
        self.assertEqual([msg["reason"] for msg in updates if msg["status"] == "disconnected"],
                         ["get_more_failed", "cursor_reopening"])

    def test_hourly_windows_start_at_the_next_wall_clock_hour(self):
        self.connect()
        self.reporter.mongodb_opened()
        self.clock.advance(23 * 60 - 1)
        self.reporter.record_change_received()
        self.pump()
        self.assertEqual(self.messages(status.MONGODB_STATUS_TOPIC, "hourly"), [])

        self.clock.advance(1)
        self.reporter.record_change_received()
        self.pump()
        first, = self.messages(status.MONGODB_STATUS_TOPIC, "hourly")
        self.assertEqual(first["window_start"], "2026-11-24T15:00:00.000Z")
        self.assertEqual(first["window_end"], "2026-11-24T16:00:00.000Z")
        self.assertEqual(first["changes_received_total"], 2)
        self.assertEqual(first["changes_received_last_hour"], 1)
        self.assertEqual(first["status"], "connected")
        self.assertEqual(first["uptime_seconds"], 23 * 60)

        self.clock.advance(HOUR)
        self.pump()
        second = self.messages(status.MONGODB_STATUS_TOPIC, "hourly")[-1]
        self.assertEqual(second["window_end"], "2026-11-24T17:00:00.000Z")
        self.assertEqual(second["changes_received_last_hour"], 1)
        self.clock.advance(HOUR)
        self.pump()
        third = self.messages(status.MONGODB_STATUS_TOPIC, "hourly")[-1]
        self.assertEqual(third["changes_received_last_hour"], 0)

    def test_wall_clock_steps_do_not_repeat_or_backfill_reports(self):
        self.connect()
        self.clock.advance(23 * 60)
        self.pump()
        self.clock.wall -= 60
        self.pump()
        self.clock.advance(60)
        self.pump()
        self.assertEqual(len(self.messages(status.MQTT_STATUS_TOPIC, "hourly")), 1)

        self.clock.wall += 2 * HOUR
        self.pump()
        first, latest = self.messages(status.MQTT_STATUS_TOPIC, "hourly")
        self.assertEqual(latest["window_start"], "2026-11-24T17:00:00.000Z")
        self.assertEqual(latest["window_end"], "2026-11-24T18:00:00.000Z")
        self.assertEqual(latest["uptime_seconds"], first["uptime_seconds"] + 60)

    def test_disconnected_uptime_and_uptime_after_reconnect(self):
        self.connect()
        self.reporter.mongodb_opened()
        self.clock.advance(10 * 60)
        self.reporter.mongodb_lost("get_more_failed")
        self.clock.advance(13 * 60)
        self.pump()
        disconnected, = self.messages(status.MONGODB_STATUS_TOPIC, "hourly")
        self.assertEqual(disconnected["status"], "disconnected")
        self.assertEqual(disconnected["mongodb_uptime_seconds"], 0)

        self.disconnect()
        self.clock.advance(HOUR - 5)
        self.connect()
        self.reporter.mongodb_opened()
        self.pump()
        self.clock.advance(5)
        self.pump()
        mqtt_report = self.messages(status.MQTT_STATUS_TOPIC, "hourly")[-1]
        mongo_report = self.messages(status.MONGODB_STATUS_TOPIC, "hourly")[-1]
        self.assertEqual(mqtt_report["mqtt_uptime_seconds"], 5)
        self.assertEqual(mongo_report["mongodb_uptime_seconds"], 5)
        self.assertEqual(mqtt_report["uptime_seconds"], HOUR + 23 * 60)

    def test_tracks_acknowledgments_failures_losses_and_pending_by_hour(self):
        self.connect()
        self.ack_changes = True
        self.send_change()
        self.ack_changes = False
        self.send_change(mqtt.MQTT_ERR_QUEUE_SIZE)
        self.disconnect()
        self.clock.advance(23 * 60 - 1)
        retry = self.send_change(mqtt.MQTT_ERR_NO_CONN)
        self.connect()
        self.clock.advance(1)
        self.pump()

        before, = self.messages(status.MQTT_STATUS_TOPIC, "hourly")
        self.assertEqual(before["change_messages_published_total"], 1)
        self.assertEqual(before["change_messages_published_last_hour"], 1)
        self.assertEqual(before["change_publish_failures_last_hour"], 2)
        self.assertEqual(before["change_messages_lost_last_hour"], 1)
        self.assertEqual(before["change_messages_pending"], 1)

        self.ack(retry.mid)
        self.ack(retry.mid)
        self.clock.advance(HOUR)
        self.pump()
        after = self.messages(status.MQTT_STATUS_TOPIC, "hourly")[-1]
        self.assertEqual(after["change_messages_published_total"], 2)
        self.assertEqual(after["change_messages_published_last_hour"], 1)
        self.assertEqual(after["change_publish_failures_total"], 2)
        self.assertEqual(after["change_publish_failures_last_hour"], 0)
        self.assertEqual(after["change_messages_lost_total"], 1)
        self.assertEqual(after["change_messages_lost_last_hour"], 0)
        self.assertEqual(after["change_messages_pending"], 0)


if __name__ == "__main__":
    unittest.main()
