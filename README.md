# ChangeStream2MQTT
Ingests MongoDB Atlas Changestream and replicates changes to MQTT

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
