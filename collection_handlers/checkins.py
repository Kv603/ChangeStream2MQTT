"""Policy for check-in changes."""

import os

from .common import convert_to_eastern, slack_user_for_card


def handle_change(database, operation, document, notifications):
    """Send notifications for a check-in insert."""
    if operation != "insert":
        return

    slackuser = slack_user_for_card(database, document)
    person = document.get("holder") or document.get("name", "")
    details = " ".join(filter(None, [str(document.get("validity", "")),
                                      str(person), slackuser]))
    when = convert_to_eastern(document.get("timeOf", document.get("time", "")))
    reader = document.get("where")
    text = f"Reader {reader} granted access" if reader else "Access granted"
    if details:
        text += f" to {details}"
    if when:
        text += f" at {when}"
    emoji = os.environ.get("SLACK_BOT_EMOJI_CHECKINS", ":robot_face:")
    notifications.run_async(notifications.push_to_slack(text, emoji))
    notifications.run_async(notifications.notify_get())

