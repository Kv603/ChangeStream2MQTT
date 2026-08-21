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


def slack_user_for_card(database, uid):
    """Return the Slack mention associated with a check-in card UID.

    The UID already comes from the change-stream document, so querying the
    event collection again adds a race-prone and potentially many-row first
    stage. The card-to-member relationship lives in ``cards``, not in the
    member's legacy ``cardID`` field. Card readers may vary the case of
    hexadecimal UIDs, and older Slack mappings may store an ObjectId as its
    string representation, so accept those equivalent representations.
    """
    if uid is None:
        return ""

    card_ids = [uid]
    if isinstance(uid, str):
        card_ids = list(dict.fromkeys((uid, uid.upper(), uid.lower())))
    card = database.cards.find_one(
        {"uid": {"$in": card_ids}}, {"member_id": 1}
    )
    if not card or card.get("member_id") is None:
        return ""

    member_id = card["member_id"]
    member_ids = list(dict.fromkeys((member_id, str(member_id))))
    slack_user = database.slack_users.find_one(
        {"member_id": {"$in": member_ids}}, {"slack_id": 1}
    )
    if not slack_user or not slack_user.get("slack_id"):
        return ""
    return f"<@{slack_user['slack_id']}>"

