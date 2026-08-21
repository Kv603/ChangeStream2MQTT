#!/usr/bin/env python3
"""Forward MongoDB change-stream events to MQTT and selected events to Slack."""

import asyncio
import logging
import logging.handlers
import os
import sys
import time

import aiohttp
import paho.mqtt.client as mqtt
import pymongo
from bson.json_util import dumps

from collection_handlers import NotificationDependencies
from collection_handlers import checkins, rejections
from collection_handlers.common import (convert_to_eastern, normalize_epoch_ms,
                                        whole_duration)


logger = logging.getLogger(__name__)
COLLECTION_HANDLERS = {
    "checkins": checkins.handle_change,
    "rejections": rejections.handle_change,
}


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
    except (aiohttp.ClientError, asyncio.TimeoutError) as error:
        logger.error("Slack webhook request failed: %s", error)
        return False
    return True


def run_async(coroutine):
    """Run an asynchronous notification from the synchronous change stream."""
    return asyncio.run(coroutine)


def slack_user_for_member(database, member_id):
    result = database.slack_users.find_one({"member_id": member_id}, {"slack_id": 1})
    return f"<@{result['slack_id']}>" if result and result.get("slack_id") else ""


def handle_slack_change(database, collection, operation, document, now_ms=None):
    """Construct and send Slack messages for supported MongoDB changes."""
    now_ms = time.time() * 1000 if now_ms is None else now_ms
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
    has_full_document = "fullDocument" in update_change
    if has_full_document:
        document = update_change["fullDocument"]
    else:
        document = update_change.get("documentKey", update_change)

    # MQTT forwarding is the primary path. Queue the event before performing
    # any synchronous database lookups or awaiting external notifications.
    publish_to_mqtt(mqttclient, collection, operation, dumps(document))

    # updateLookup can return null when the document disappears before MongoDB
    # resolves it. The null event has already reached MQTT; it has no Slack
    # document to inspect.
    if document is None:
        return

    handler = COLLECTION_HANDLERS.get(collection)
    if handler:
        handler(database, operation, document, NotificationDependencies(
            push_to_slack=push_to_slack,
            notify_get=notify_get,
            run_async=run_async,
            now_ms=lambda: time.time() * 1000,
        ))
    if has_full_document:
        handle_slack_change(database, collection, operation, document)


def publish_to_mqtt(mqttclient, collection, operation, document):
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
