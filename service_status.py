"""Publish retained service status and account for MQTT change deliveries."""

import json
import logging
import platform
import socket
import subprocess
import sys
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from queue import Empty, SimpleQueue
from threading import Event, Lock, Thread

import paho.mqtt.client as mqtt
from pymongo.monitoring import CommandListener


logger = logging.getLogger(__name__)
STATUS_TOPIC = "ChangeStream2MQTT/status"
MQTT_STATUS_TOPIC = f"{STATUS_TOPIC}/mqtt"
MONGODB_STATUS_TOPIC = f"{STATUS_TOPIC}/mongodb"
HOUR = 3600
SHUTDOWN_TIMEOUT = 2

MQTT_COUNTERS = (
    "change_messages_published",
    "change_publish_failures",
    "change_messages_lost",
)
MONGODB_COUNTERS = ("changes_received",)


def _hour_start(seconds):
    return int(seconds // HOUR) * HOUR


def _timestamp(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def _startup_details(environment):
    try:
        uname = subprocess.run(
            ["uname", "-a"], check=True, capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=2,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        uname = " ".join(platform.uname())

    executable = Path(sys.executable).resolve()
    entrypoint = Path(sys.argv[0]).resolve()
    settings = {
        "MQTT_HOST": environment["MQTT_HOST"],
        "MQTT_USER": environment.get("MQTT_USER"),
    }
    if environment.get("LOGHOST"):
        settings["LOGHOST"] = environment["LOGHOST"]
    return {
        "uname": uname,
        "executable": {"name": executable.name, "path": str(executable)},
        "entrypoint": {"name": entrypoint.name, "path": str(entrypoint)},
        "environment": settings,
    }


class StatusReporter:
    """Use one worker for connection updates, retained messages and hourly reports."""

    def __init__(self, client, environment, *, wall_clock=time.time,
                 monotonic=time.monotonic):
        self._client = client
        self._wall_clock = wall_clock
        self._monotonic = monotonic
        start_wall = wall_clock()
        self._started = monotonic()
        self._hostname = socket.gethostname()
        self._startup = _startup_details(environment)
        self._events = SimpleQueue()
        self._event_lock = Lock()
        self._halt = Event()
        self._worker = Thread(target=self._run, name="mqtt-status", daemon=True)
        self._closing = False

        # Serialize publish and packet registration. Paho callbacks only enqueue
        # work because Paho can invoke them with its own publish mutex held.
        self._lock = Lock()
        self._pending_packets = defaultdict(deque)
        self._pending_changes = 0
        self._totals = Counter()
        self._hours = defaultdict(Counter)
        self._reported_through = _hour_start(start_wall)
        self._scheduled_through = self._reported_through

        self._outbox = {topic: deque() for topic in (
            STATUS_TOPIC, MQTT_STATUS_TOPIC, MONGODB_STATUS_TOPIC,
        )}
        self._status_inflight = set()
        self._mqtt_connected_at = None
        self._mqtt_ever_connected = False
        self._mongodb_connected_at = None
        self._mongodb_ever_connected = False
        self._mongodb_state = "connecting"
        self._mongodb_latest = None

        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_publish = self._on_publish
        will = self._message(
            wall_clock(), "connection_lost", "disconnected",
            message="connection lost", timestamp_kind="will_configured",
        )
        client.will_set(MQTT_STATUS_TOPIC, json.dumps(will), qos=1, retain=True)

    def start(self):
        self._worker.start()

    def close(self):
        self._post(self._shutdown)
        self._worker.join(timeout=SHUTDOWN_TIMEOUT)
        if self._worker.is_alive():
            self._halt.set()
            self._events.put(None)
            self._worker.join(timeout=1)

    def publish(self, topic, payload, qos):
        """Forward a change through the same interface as the MQTT client."""
        return self._submit(topic, payload, qos, retain=False)

    def record_change_received(self):
        with self._lock:
            self._count(self._wall_clock(), "changes_received")

    def mongodb_opened(self):
        self._post(self._mongodb_opened)

    def mongodb_lost(self, reason):
        self._post(self._mongodb_lost, reason)

    def _message(self, wall, event, status, **fields):
        return {
            "timestamp": _timestamp(wall), "hostname": self._hostname,
            "event": event, "status": status, **fields,
        }

    def _post(self, handler, *args):
        # Timestamp events in queue order so an hourly snapshot accounts for
        # acknowledgments posted before its boundary.
        with self._event_lock:
            self._events.put((handler, (self._wall_clock(), self._monotonic(), *args)))

    def _on_connect(self, _client, _userdata, _flags, reason, _properties):
        if not reason.is_failure:
            self._post(self._mqtt_opened)

    def _on_disconnect(self, _client, _userdata, _flags, _reason, _properties):
        self._post(self._mqtt_lost)

    def _on_publish(self, _client, _userdata, mid, reason, _properties):
        self._post(self._acknowledge, mid, reason.is_failure)

    def _count(self, wall, *counters):
        self._totals.update(counters)
        boundary = _hour_start(wall) + HOUR
        if boundary > self._reported_through:
            self._hours[boundary].update(counters)

    def _submit(self, topic, payload, qos, *, retain):
        with self._lock:
            try:
                result = self._client.publish(topic, payload, qos, retain=retain)
            except Exception as error:
                if not retain:
                    counters = ("change_publish_failures",)
                    if isinstance(error, ValueError):
                        counters += ("change_messages_lost",)
                    self._count(self._wall_clock(), *counters)
                raise

            rejected = result.rc == mqtt.MQTT_ERR_QUEUE_SIZE
            if not retain and result.rc != mqtt.MQTT_ERR_SUCCESS:
                counters = ("change_publish_failures",)
                if rejected:
                    counters += ("change_messages_lost",)
                self._count(self._wall_clock(), *counters)
            if not rejected:
                # Paho can still deliver a QoS 1/2 packet after MQTT_ERR_NO_CONN.
                self._pending_packets[result.mid].append(topic if retain else None)
                if not retain:
                    self._pending_changes += 1
            return result

    def _acknowledge(self, wall, _monotonic, mid, failed):
        with self._lock:
            packets = self._pending_packets.get(mid)
            if not packets:
                return
            topic = packets.popleft()
            if not packets:
                del self._pending_packets[mid]
            if topic is None:
                self._pending_changes -= 1
                if failed:
                    self._count(wall, "change_publish_failures", "change_messages_lost")
                else:
                    self._count(wall, "change_messages_published")
            else:
                self._status_inflight.discard(topic)
        if topic is not None and failed:
            logger.warning("Broker rejected status on %s", topic)

    def _mqtt_opened(self, wall, monotonic):
        if self._mqtt_connected_at is not None:
            return
        self._mqtt_connected_at = monotonic
        event = "connection_resumed" if self._mqtt_ever_connected else "connection_established"
        if not self._mqtt_ever_connected:
            self._mqtt_ever_connected = True
            self._outbox[STATUS_TOPIC].append(
                self._message(wall, "startup", "started", **self._startup)
            )
        self._outbox[MQTT_STATUS_TOPIC].append(self._message(wall, event, "connected"))
        if self._mongodb_latest is not None:
            self._outbox[MONGODB_STATUS_TOPIC].append(self._mongodb_latest)
        if self._closing:
            self._outbox[MQTT_STATUS_TOPIC].append(
                self._message(wall, "shutdown", "disconnected")
            )

    def _mqtt_lost(self, _wall, _monotonic):
        self._mqtt_connected_at = None
        self._outbox[MQTT_STATUS_TOPIC].clear()
        self._outbox[MONGODB_STATUS_TOPIC].clear()

    def _mongodb_opened(self, wall, monotonic):
        if self._mongodb_state == "connected":
            return
        event = "connection_resumed" if self._mongodb_ever_connected else "connection_established"
        self._mongodb_ever_connected = True
        self._mongodb_connected_at = monotonic
        self._mongodb_state = "connected"
        self._mongodb_update(self._message(wall, event, self._mongodb_state))

    def _mongodb_lost(self, wall, _monotonic, reason):
        if self._mongodb_state == "disconnected":
            return
        self._mongodb_connected_at = None
        self._mongodb_state = "disconnected"
        self._mongodb_update(
            self._message(wall, "connection_lost", self._mongodb_state, reason=reason)
        )

    def _mongodb_update(self, message):
        self._mongodb_latest = message
        if self._mqtt_connected_at is not None:
            self._outbox[MONGODB_STATUS_TOPIC].append(message)

    def _flush_status(self):
        if self._mqtt_connected_at is None or not self._client.is_connected():
            return

        for topic, messages in self._outbox.items():
            if not messages or topic in self._status_inflight:
                continue
            try:
                result = self._submit(topic, json.dumps(messages[0]), 1, retain=True)
            except (OSError, RuntimeError, ValueError) as error:
                logger.warning("Could not submit status on %s (%s)", topic,
                               type(error).__name__)
                messages.popleft()
                continue

            if result.rc != mqtt.MQTT_ERR_QUEUE_SIZE:
                messages.popleft()
                self._status_inflight.add(topic)

    def _report_hour(self, _wall, _monotonic, boundary):
        if boundary != self._scheduled_through:
            return
        with self._lock:
            hourly = self._hours.get(boundary, Counter())
            self._hours = defaultdict(Counter, (
                (end, counts) for end, counts in self._hours.items() if end > boundary
            ))
            self._reported_through = boundary
            totals = self._totals.copy()
            pending = self._pending_changes

        if self._closing or self._mqtt_connected_at is None or not self._client.is_connected():
            return

        wall = self._wall_clock()
        monotonic = self._monotonic()
        shared = {
            "window_start": _timestamp(boundary - HOUR),
            "window_end": _timestamp(boundary),
            "uptime_seconds": int(monotonic - self._started),
        }
        for topic, state, connected_at, component, counters in (
            (MQTT_STATUS_TOPIC, "connected", self._mqtt_connected_at, "mqtt", MQTT_COUNTERS),
            (MONGODB_STATUS_TOPIC, self._mongodb_state, self._mongodb_connected_at,
             "mongodb", MONGODB_COUNTERS),
        ):
            message = self._message(wall, "hourly", state, **shared)
            message[f"{component}_uptime_seconds"] = (
                0 if connected_at is None else int(monotonic - connected_at)
            )
            for counter in counters:
                message[f"{counter}_total"] = totals[counter]
                message[f"{counter}_last_hour"] = hourly[counter]
            if topic == MQTT_STATUS_TOPIC:
                message["change_messages_pending"] = pending
            self._outbox[topic].append(message)

    def _shutdown(self, wall, monotonic):
        self._closing = True
        if self._mongodb_state == "connected":
            self._mongodb_lost(wall, monotonic, "shutdown")
        if self._mqtt_connected_at is not None:
            self._outbox[MQTT_STATUS_TOPIC].append(
                self._message(wall, "shutdown", "disconnected")
            )

    def _step(self, max_wait=1):
        wall = self._wall_clock()
        boundary = _hour_start(wall)
        if boundary > self._scheduled_through:
            self._scheduled_through = boundary
            # Put the snapshot behind acknowledgments already in the queue.
            self._post(self._report_hour, boundary)
        delay = min(max_wait, self._scheduled_through + HOUR - wall)
        try:
            event = self._events.get(timeout=delay)
        except Empty:
            pass
        else:
            if event is None or self._halt.is_set():
                return False
            handler, args = event
            handler(*args)
        self._flush_status()
        return not (self._closing and self._mqtt_ever_connected
                    and not self._status_inflight and not any(self._outbox.values()))

    def _run(self):
        while not self._halt.is_set() and self._step():
            pass


class MongoStreamListener(CommandListener):
    """Observe the watch cursor, including recovery handled inside PyMongo."""

    def __init__(self, reporter, database_name):
        self._reporter = reporter
        self._database_name = database_name
        self._requests = {}
        self._cursor = None
        self._opened = False

    def started(self, event):
        if event.database_name != self._database_name:
            return
        request = (event.connection_id, event.request_id)
        command = event.command
        if event.command_name == "aggregate":
            pipeline = command.get("pipeline")
            if (command.get("aggregate") != 1 or not pipeline
                    or "$changeStream" not in pipeline[0]):
                return
            self._requests[request] = "aggregate"
            if self._opened:
                self._reporter.mongodb_lost("cursor_reopening")
        elif event.command_name == "getMore":
            if self._cursor == (event.connection_id, command.get("getMore")):
                self._requests[request] = "getMore"

    def succeeded(self, event):
        command = self._requests.pop((event.connection_id, event.request_id), None)
        if command is None:
            return
        cursor_id = event.reply.get("cursor", {}).get("id")
        self._cursor = (event.connection_id, cursor_id) if cursor_id else None
        if command == "aggregate" and self._cursor is not None:
            self._opened = True
            self._reporter.mongodb_opened()

    def failed(self, event):
        command = self._requests.pop((event.connection_id, event.request_id), None)
        if command == "getMore":
            self._cursor = None
            self._reporter.mongodb_lost("get_more_failed")
        elif command == "aggregate":
            self._reporter.mongodb_lost("watch_error")
