"""Formatting and lookup helpers shared by collection handlers."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")


def normalize_epoch_ms(value):
    """Accept epoch seconds or milliseconds and return milliseconds."""
    value = float(value)
    return value * 1000 if value < 100_000_000_000 else value


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

