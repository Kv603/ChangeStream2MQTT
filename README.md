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

## Slack user cache

Successful card-to-Slack-user lookups are cached in RAM. Cache keys use the
combination of the `holder`, `name`, and `uid` fields present on each event,
with card UIDs normalized to uppercase and names normalized for case and
whitespace. Repeated events with the same identity fields therefore avoid the
MongoDB lookup.

Set `CACHEDIR` to persist the cache between process restarts. The service
creates `slack_user_cache.json` under that directory and updates it atomically
after a successful lookup. If `CACHEDIR` is unset or empty, the cache remains
in RAM only. Delete the file and restart the service to clear persisted entries
after a card or Slack-user mapping changes. Invalid cache files are renamed
with a `.bad` extension and ignored so event processing can continue.
