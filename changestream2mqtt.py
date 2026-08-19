#!/usr/bin/env python3
"""Forward MongoDB change-stream events to MQTT and selected events to Slack."""

import asyncio
import logging
import logging.handlers
import os
import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import aiohttp
import paho.mqtt.client as mqtt
import pymongo
from bson.json_util import dumps


logger = logging.getLogger(__name__)
EASTERN = ZoneInfo("America/New_York")


def configure_logging():
    """Configure local or remote logging."""
    if os.environ.get("LOGHOST"):
        handler = logging.handlers.SysLogHandler(
            address=(os.environ["LOGHOST"], 514)
        )
        handler.setFormatter(logging.Formatter(
            f"%(asctime)s - %(levelname)s - {sys.argv[0]} %(message)s"
        ))
        logging.getLogger().setLevel(logging.WARNING)
        logging.getLogger().addHandler(handler)
    else:
        logging.basicConfig(stream=sys.stderr, level=logging.INFO)


async def notify_get():
    """Notify the optional legacy external service."""
    url = os.environ.get("REMOTE_GET_URL")
    if url:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                logger.info("Web Service said %s", await response.text())


async def push_to_slack(text, emoji):
    """Post a Slack webhook message, or safely skip an incomplete setup."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    channel = os.environ.get("SLACK_CHANNEL")
    missing = [name for name, value in (
        ("SLACK_WEBHOOK_URL", webhook_url), ("SLACK_CHANNEL", channel)
    ) if not value]
    if missing:
        logger.warning("Slack notification skipped; missing setting(s): %s",
                       ", ".join(missing))
        return False

    payload = {
        "channel": channel,
        "username": os.environ.get("SLACK_BOT_USERNAME", "DoorBoto"),
        "icon_emoji": emoji,
        "text": text,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(webhook_url, json=payload) as response:
                body = await response.text()
                if response.status >= 400:
                    logger.warning("Slack webhook returned HTTP %s: %s",
                                   response.status, body)
                    return False
    except aiohttp.ClientError as error:
        logger.warning("Slack webhook request failed: %s", error)
        return False
    return True


def run_async(coroutine):
    """Run an asynchronous notification from the synchronous change stream."""
    return asyncio.run(coroutine)


def convert_to_eastern(value):
    """Format a BSON datetime or epoch timestamp in US Eastern time."""
    if isinstance(value, dict) and "$date" in value:
        value = value["$date"]
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    elif isinstance(value, (int, float)):
        value = datetime.fromtimestamp(normalize_epoch_ms(value) / 1000,
                                       tz=timezone.utc)
    if not isinstance(value, datetime):
        return str(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(EASTERN).strftime("%Y-%m-%d %I:%M:%S %p %Z")


def normalize_epoch_ms(value):
    """Accept epoch seconds or milliseconds and return milliseconds."""
    value = float(value)
    return value * 1000 if value < 100_000_000_000 else value


def whole_duration(milliseconds):
    """Describe a duration using its largest whole conventional unit."""
    units = (
        (365 * 24 * 60 * 60 * 1000, "year"),
        (30 * 24 * 60 * 60 * 1000, "month"),
        (7 * 24 * 60 * 60 * 1000, "week"),
        (24 * 60 * 60 * 1000, "day"),
        (60 * 60 * 1000, "hour"),
        (60 * 1000, "minute"),
        (1000, "second"),
    )
    for unit_ms, name in units:
        if milliseconds >= unit_ms:
            count = int(milliseconds // unit_ms)
            return f"{count} {name}{'' if count == 1 else 's'}"
    return "0 seconds"


def slack_user_for_card(database, collection, uid):
    """Return a Slack mention associated with a card ID."""
    pipeline = [
        {"$match": {"uid": uid}},
        {"$lookup": {"from": "members", "localField": "uid",
                     "foreignField": "cardID", "as": "member"}},
        {"$unwind": {"path": "$member", "preserveNullAndEmptyArrays": False}},
        {"$lookup": {"from": "slack_users", "localField": "member._id",
                     "foreignField": "member_id", "as": "slack_user"}},
        {"$unwind": {"path": "$slack_user",
                     "preserveNullAndEmptyArrays": False}},
        {"$addFields": {"slack_id": "$slack_user.slack_id"}},
        {"$project": {"slack_user": 0}},
    ]
    result = next(iter(database[collection].aggregate(pipeline)), None)
    return f"<@{result['slack_id']}>" if result and result.get("slack_id") else ""


def slack_user_for_member(database, member_id):
    result = database.slack_users.find_one({"member_id": member_id}, {"slack_id": 1})
    return f"<@{result['slack_id']}>" if result and result.get("slack_id") else ""


def handle_slack_change(database, collection, operation, document, now_ms=None):
    """Construct and send Slack messages for supported MongoDB changes."""
    now_ms = time.time() * 1000 if now_ms is None else now_ms
    if collection in ("rejections", "checkins") and operation == "insert":
        slackuser = ""
        if document.get("holder") is not None:
            slackuser = slack_user_for_card(database, collection, document.get("uid"))
        details = " ".join(filter(None, [str(document.get("validity", "")),
                                          str(document.get("holder", "")), slackuser]))
        when = convert_to_eastern(document.get("timeOf", document.get("time", "")))
        if collection == "rejections":
            text = f"Reader {document.get('where', '')} rejected {details} at {when}"
            expiry = document.get("expiry")
            if expiry is not None and float(expiry) > 0:
                expiry_ms = normalize_epoch_ms(expiry)
                if expiry_ms < now_ms:
                    text += f" (expired {whole_duration(now_ms - expiry_ms)} ago)"
            emoji = os.environ.get("SLACK_BOT_EMOJI_REJECTIONS", ":octagonal_sign:")
        else:
            text = (f"Reader {document.get('where', '')} granted access to "
                    f"{details} at {when}")
            emoji = os.environ.get("SLACK_BOT_EMOJI_CHECKINS", ":robot_face:")
        return run_async(push_to_slack(text, emoji))

    if collection == "members" and operation in ("insert", "update", "replace"):
        expiry = document.get("expirationTime")
        if (document.get("status") == "activeMember" and document.get("cardID")
                and expiry is not None and normalize_epoch_ms(expiry) > now_ms):
            expires = whole_duration(normalize_epoch_ms(expiry) - now_ms)
            slackuser = slack_user_for_member(database, document.get("_id"))
            name = " ".join(filter(None, [document.get("firstname", ""),
                                           document.get("lastname", ""), slackuser]))
            text = f"{document['status']} {name} now expires in {expires}"
            emoji = os.environ.get("SLACK_BOT_EMOJI_ACTIVEMEMBER", ":+1:")
            return run_async(push_to_slack(text, emoji))
    return False


def process_mongo_update(update_change, database, mqttclient):
    collection = update_change["ns"]["coll"]
    operation = update_change["operationType"]
    if "fullDocument" in update_change:
        document = update_change["fullDocument"]
        handle_slack_change(database, collection, operation, document)
    else:
        document = update_change.get("documentKey", update_change)
    publish_to_mqtt(mqttclient, collection, operation, dumps(document))


def publish_to_mqtt(mqttclient, collection, operation, document):
    if collection == "checkins" and operation == "insert":
        run_async(notify_get())
    topic = f"{collection}/{operation}"
    result = mqttclient.publish(
        topic, f'{operation} {int(time.time())} {{"document": {document} }}', 2
    )
    if result[0] == 0:
        logger.info("%s to %s %s OK", operation, topic, document)
    else:
        logger.warning("%s to %s %s failed", operation, topic, document)


def main():
    configure_logging()
    mqttclient = mqtt.Client()
    if os.environ.get("MQTT_USER"):
        mqttclient.username_pw_set(os.environ["MQTT_USER"], os.environ.get("MQTT_PW"))
    mqttclient.connect(os.environ["MQTT_HOST"], int(os.environ.get("MQTT_PORT", 1883)), 60)
    mqttclient.loop_start()

    client = pymongo.MongoClient(os.environ["CHANGE_STREAM_DB"])
    client.admin.command("ping")
    database = client.makerauth
    try:
        with database.watch([], full_document="updateLookup") as change_stream:
            for change in change_stream:
                process_mongo_update(change, database, mqttclient)
    finally:
        client.close()
        mqttclient.disconnect()


if __name__ == "__main__":
    main()
