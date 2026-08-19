# ChangeStream2MQTT
Ingests MongoDB Atlas Changestream and replicates changes to MQTT

Selected `checkins`, `rejections`, and active `members` changes can also be sent
to Slack through an incoming webhook. Set both required variables:

* `SLACK_WEBHOOK_URL` - the incoming webhook URL
* `SLACK_CHANNEL` - the destination channel

Slack notifications are skipped with a warning if either setting is absent.
The optional `SLACK_BOT_USERNAME` defaults to `DoorBoto`. Event icons can be
customized with `SLACK_BOT_EMOJI_CHECKINS`, `SLACK_BOT_EMOJI_REJECTIONS`, and
`SLACK_BOT_EMOJI_ACTIVEMEMBER`.
