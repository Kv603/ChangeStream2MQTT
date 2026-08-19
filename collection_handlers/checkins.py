"""Policy for check-in changes."""

import os

from .common import convert_to_eastern, slack_user_for_card


def handle_change(database, operation, document, notifications):
    """Send notifications for a check-in insert."""
    if operation != "insert":
        return

    slackuser = ""
    if document.get("holder") is not None:
        slackuser = slack_user_for_card(database, "checkins", document.get("uid"))
    details = " ".join(filter(None, [str(document.get("validity", "")),
                                      str(document.get("holder", "")), slackuser]))
    when = convert_to_eastern(document.get("timeOf", document.get("time", "")))
    text = (f"Reader {document.get('where', '')} granted access to "
            f"{details} at {when}")
    emoji = os.environ.get("SLACK_BOT_EMOJI_CHECKINS", ":robot_face:")
    notifications.run_async(notifications.push_to_slack(text, emoji))
    notifications.run_async(notifications.notify_get())

