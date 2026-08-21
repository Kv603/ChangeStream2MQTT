"""Formatting and lookup helpers shared by collection handlers."""

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")
logger = logging.getLogger(__name__)

SLACK_USER_CACHE_FILENAME = "slack_user_cache.json"
_CACHE_FIELDS = ("holder", "name", "uid")
_CACHE_NOT_LOADED = object()
_slack_user_cache = {}
_slack_user_cache_loaded_from = _CACHE_NOT_LOADED
_slack_user_cache_lock = threading.Lock()


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


def _cache_key(identity):
    """Build a stable key from the identity fields present in a document."""
    source = identity if isinstance(identity, dict) else {"uid": identity}
    key = []
    for field in _CACHE_FIELDS:
        if field not in source or source[field] is None:
            continue
        value = " ".join(str(source[field]).split())
        if not value:
            continue
        value = value.upper() if field == "uid" else value.casefold()
        key.append((field, value))
    return tuple(key)


def _cache_file():
    cache_dir = os.environ.get("CACHEDIR")
    return (Path(cache_dir) / SLACK_USER_CACHE_FILENAME) if cache_dir else None


def _ensure_slack_user_cache_loaded():
    """Load the optional disk cache once for the configured cache directory."""
    global _slack_user_cache_loaded_from

    path = _cache_file()
    with _slack_user_cache_lock:
        if _slack_user_cache_loaded_from == path:
            return
        _slack_user_cache_loaded_from = path
        if path is None:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            if not path.exists():
                return
            payload = json.loads(path.read_text(encoding="utf-8"))
            entries = payload.get("entries", [])
            if payload.get("version") != 1 or not isinstance(entries, list):
                raise ValueError("unsupported Slack user cache format")
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                key = _cache_key(entry.get("identity", {}))
                slack_user = entry.get("slack_user")
                if key and isinstance(slack_user, str) and slack_user:
                    _slack_user_cache[key] = slack_user
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
            logger.warning("Could not load Slack user cache %s: %s", path, error)


def _persist_slack_user_cache():
    """Atomically persist a snapshot when CACHEDIR is configured."""
    path = _cache_file()
    if path is None:
        return

    with _slack_user_cache_lock:
        entries = [
            {"identity": dict(key), "slack_user": slack_user}
            for key, slack_user in sorted(_slack_user_cache.items())
        ]
    temporary_path = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with temporary_path.open(mode="w", encoding="utf-8") as temporary_file:
            json.dump({"version": 1, "entries": entries}, temporary_file,
                      indent=2, sort_keys=True)
            temporary_file.write("\n")
        os.replace(temporary_path, path)
        temporary_path = None
    except OSError as error:
        logger.warning("Could not persist Slack user cache %s: %s", path, error)
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass


def _reset_slack_user_cache():
    """Reset process-local cache state; intended for isolated tests."""
    global _slack_user_cache_loaded_from

    with _slack_user_cache_lock:
        _slack_user_cache.clear()
        _slack_user_cache_loaded_from = _CACHE_NOT_LOADED


def slack_user_for_card(database, identity):
    """Return the Slack mention associated with a check-in card UID.

    Successful lookups are cached by the combination of ``holder``, ``name``,
    and ``uid`` present in *identity*. A bare UID remains supported for callers
    that do not have the complete event document.

    The UID already comes from the change-stream document, so querying the
    event collection again adds a race-prone and potentially many-row first
    stage. The card-to-member relationship lives in ``cards``, not in the
    member's legacy ``cardID`` field. Card readers may vary the case of
    hexadecimal UIDs, and older Slack mappings may store an ObjectId as its
    string representation, so accept those equivalent representations.
    """
    _ensure_slack_user_cache_loaded()
    key = _cache_key(identity)
    if key:
        with _slack_user_cache_lock:
            cached_user = _slack_user_cache.get(key)
        if cached_user is not None:
            return cached_user

    uid = identity.get("uid") if isinstance(identity, dict) else identity
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
    mention = f"<@{slack_user['slack_id']}>"
    if key:
        with _slack_user_cache_lock:
            _slack_user_cache[key] = mention
        _persist_slack_user_cache()
    return mention

