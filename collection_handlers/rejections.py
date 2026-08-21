"""Policy for rejection changes."""

import os

from .common import (convert_to_eastern, normalize_epoch_ms,
                     slack_user_for_card, whole_duration)


def handle_change(database, operation, document, notifications):
    """Send a Slack notification for a rejection insert."""
    if operation != "insert":
        return

    slackuser = ""
    if document.get("holder") is not None:
        slackuser = slack_user_for_card(database, document.get("uid"))
    details = " ".join(filter(None, [str(document.get("validity", "")),
                                      str(document.get("holder", "")), slackuser]))
    when = convert_to_eastern(document.get("timeOf", document.get("time", "")))
    text = f"Reader {document.get('where', '')} rejected {details} at {when}"
    expiry = document.get("expiry")
    now_ms = notifications.now_ms()
    if expiry is not None and float(expiry) > 0:
        expiry_ms = normalize_epoch_ms(expiry)
        if expiry_ms < now_ms:
            text += f" (expired {whole_duration(now_ms - expiry_ms)} ago)"
    emoji = os.environ.get("SLACK_BOT_EMOJI_REJECTIONS", ":octagonal_sign:")
    notifications.run_async(notifications.push_to_slack(text, emoji))
