# ChangeStream2MQTT
Ingests MongoDB Atlas Changestream and replicates changes to MQTT

The bridge skips events from collections whose names start with `ledger_`
before forwarding, logging payloads, or sending Slack notifications. Direct
calls to the MQTT publisher also skip these collections. The Ledger publishes
its own minimal advancement events. The prefix check keeps private consent
records, recognition messages, prompts, and game records out of the bridge's
output.

## Docker configuration

Create `changestream2mqtt.env` with the connection settings for your
deployment. Replace these example endpoints with ones reachable from the
container.

```dotenv
CHANGE_STREAM_DB=mongodb://mongo.example.com:27017/?replicaSet=rs0
MQTT_HOST=mqtt.example.com
```

```sh
docker build -t changestream2mqtt .
docker run --env-file changestream2mqtt.env changestream2mqtt
```

### Environment variables

| Variable | Requirement | Default | Purpose |
| --- | --- | --- | --- |
| `CHANGE_STREAM_DB` | Required | Unset | MongoDB connection URI. The service watches the `makerauth` database. |
| `MQTT_HOST` | Optional | `127.0.0.1` | MQTT broker hostname or IP. Set this if the broker is unavailable at the container's loopback address. |
| `MQTT_PORT` | Optional | `1883` | MQTT broker port. |
| `MQTT_USER` | Required if the broker requires a username | Empty | MQTT username. An empty value disables MQTT authentication. |
| `MQTT_PW` | Required if the broker requires a password | Empty | MQTT password. The application uses it only with a non-empty `MQTT_USER`. |
| `LOGHOST` | Optional | Unset | Syslog destination for warnings and errors over UDP port `514`. When unset, the service logs to stderr at `INFO` level. |
| `REMOTE_GET_URL` | Optional | Unset | Legacy notification URL called with `GET` for `checkins` inserts. |
| `SLACK_WEBHOOK_URL` | Required for Slack notifications | Unset | Slack incoming webhook URL. |
| `SLACK_CHANNEL` | Required for Slack notifications | Unset | Channel value sent in the Slack webhook payload. |
| `SLACK_BOT_USERNAME` | Optional | `DoorBoto` | Username sent in Slack webhook payloads. |
| `SLACK_BOT_EMOJI_CHECKINS` | Optional | `:robot_face:` | Slack icon emoji for check-in notifications. |
| `SLACK_BOT_EMOJI_REJECTIONS` | Optional | `:octagonal_sign:` | Slack icon emoji for rejection notifications. |
| `SLACK_BOT_EMOJI_ACTIVEMEMBER` | Optional | `:+1:` | Slack icon emoji for active member notifications. |
| `CACHEDIR` | Optional | Unset | Writable container directory for the Slack user cache. |

Selected `checkins`, `rejections`, and active `members` changes can also be sent
to Slack through an incoming webhook. If either `SLACK_WEBHOOK_URL` or
`SLACK_CHANNEL` is missing, the service logs a warning and skips the Slack
notification.

## MQTT service status

Grant the service's MQTT account permission to publish to these topics,
including its Last Will on `ChangeStream2MQTT/status/mqtt`. The broker keeps
one retained QoS 1 message per topic. Subscribers can use `hostname` to
identify the publishing host or container.

| Topic | Events |
| --- | --- |
| `ChangeStream2MQTT/status` | `startup` on the first accepted MQTT connection. |
| `ChangeStream2MQTT/status/mqtt` | `connection_established`, `connection_resumed`, `connection_lost` via Last Will, `shutdown` on graceful exit, and `hourly`. |
| `ChangeStream2MQTT/status/mongodb` | `connection_established`, `connection_lost`, `connection_resumed`, and `hourly`. |

Every JSON message has a UTC `timestamp`, `hostname`, `event`, and `status`.
Connection status values include `connecting`, `connected`, and
`disconnected`. Startup uses `started` and adds `uname`, `executable`,
`entrypoint`, and `environment`. The `uname` field contains the output of
`uname -a`, with Python platform information as a fallback. The executable
and entry point each include a `name` and absolute `path`. The environment
object contains `MQTT_HOST`, `MQTT_USER`, and `LOGHOST` when configured.

The MQTT Last Will contains `message` set to `connection lost` and
`timestamp_kind` set to `will_configured`. Its timestamp records when the
service prepared the Will. The broker publishes that payload after an
unexpected disconnect. MongoDB loss messages include a short `reason`.
If PyMongo exhausts its recovery attempts, the service reports the stream
loss and exits with the original error.

### Hourly reports

The service reports at `:00` wall-clock time, starting at the next top of
the hour, even when MongoDB is idle. Each report includes UTC
`window_start` and `window_end` for the preceding clock hour. A change that
occurs exactly at `window_end` belongs to the next hour. The first report
covers activity since startup within that clock hour. Missed hourly reports
are skipped during a broker outage. The latest MongoDB connection state is
sent when MQTT reconnects.

Both reports include `uptime_seconds`. `mqtt_uptime_seconds` and
`mongodb_uptime_seconds` measure the current uninterrupted connection and
are zero while disconnected. For each counter below, `_total` covers the
current process lifetime and `_last_hour` covers the reported clock hour.

| Topic suffix | Counter | Meaning |
| --- | --- | --- |
| `/mqtt` | `change_messages_published` | Change publishes acknowledged by the broker through QoS 2, counted in the hour of acknowledgment. |
| `/mqtt` | `change_publish_failures` | Publish errors, including a disconnected submission that Paho may deliver later. |
| `/mqtt` | `change_messages_lost` | Definite publish rejections, such as a full client queue or invalid publish arguments. |
| `/mongodb` | `changes_received` | Events returned by the change stream, counted before collection filtering. |

MQTT reports also include `change_messages_pending`, the current count
awaiting a broker acknowledgment. A pending change can increase the
published counter after a reconnect. Intentional collection skips and status
publishes are excluded from the MQTT change counters.

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

Mount a Docker volume at the `CACHEDIR` path to keep the cache when the
container is replaced.
