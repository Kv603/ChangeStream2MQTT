#! /usr/bin/python3
# watch MongoDB changestream for entire database
#    https://www.mongodb.com/developer/languages/python/python-change-streams/
# Extract change documents
# Publish changes to MQTT
#

import os
import pymongo
from bson.json_util import dumps
import paho.mqtt.client as mqtt
import time
import aiohttp
import asyncio
import sys
import logging
import logging.handlers

# Set up logging to remote syslog server
if os.environ.get("LOGHOST"):
    progname=sys.argv[0]
    syslog_handler = logging.handlers.SysLogHandler(address=(os.environ.get("LOGHOST") , 514))
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - {progname} %(message)s')
    logger = logging.getLogger()
    logger.setLevel(logging.WARNING)
    logger.addHandler(syslog_handler)
else:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)


# Notify via URL call to an external service
async def notify_get():
    url=os.environ.get("REMOTE_GET_URL")
    if url:
        print("Calling External GET url")
        async with aiohttp.ClientSession() as session: 
            async with session.get(url) as resp:
                resp = await resp.text()
                logger.info(f"Web Service said {resp}")


# Deal with incoming change depending on available contents
def process_mongo_update(update_change):
    if "fullDocument" in update_change:
        publish_to_mqtt(update_change["ns"]["coll"], update_change["operationType"], dumps(update_change["fullDocument"]))
    elif "documentKey" in update_change:
        publish_to_mqtt(update_change["ns"]["coll"],update_change["operationType"], dumps(update_change["documentKey"]))
    else:
        publish_to_mqtt(update_change["ns"]["coll"], update_change["operationType"], dumps(update_change))

go_publish=False


# Publish to a topic in MQTT
def publish_to_mqtt(coll,op,doc):
    if("checkins" == coll and  "insert" == op):
        print("Notify on checkins (async)")
        asyncio.run(notify_get())
    print(doc)
    topic= coll + "/" + op
    ret=mqttclient.publish(topic,op + " " + str(int(time.time())) + " {\"document\": " + doc + " }" ,2)
    status = ret[0]
    if status == 0:
        print("Sent " + op + " to topic "+topic)
        logger.info(f"{op} to {topic} {doc} OK")
    else:
        print("Failed sending " + op + " to topic "+topic)
        logger.warning(f"{op} to {topic} {doc} failed")
    print('') # for readability only


# Action when our connection to MQTT is established
def on_connect_mqtt(mclient, userdata, flags, rc):
    if(0==rc):
        print("MQTT connected with result code "+str(rc))
        print(mqtt.connack_string(rc))
        logger.info(f"Connected to "+ os.environ['MQTT_HOST'])
        mclient.subscribe("$SYS/#")
        go_publish=True
    else:
        print("MQTT connect failed with result code "+str(rc))
        logger.error("MQTT connect failed with result code "+str(rc))
        time.sleep(13)
        exit(rc)


# When MQTT is lost
# Note that the client library will reconnect when possible!
def on_disconnect_mqtt(client, userdata, rc):
    print("MQTT disconnected with rtn code [%d]"% (rc) )
    logger.error("MQTT dropped with rtn code "+str(rc))
    go_publish=False


# First get our MQTT ready
mqttclient= mqtt.Client()
mqttclient.on_connect_mqtt = on_connect_mqtt
mqttclient.on_disconnect_mqtt = on_disconnect_mqtt
if os.environ['MQTT_USER']:
    mqttclient.username_pw_set(username=os.environ['MQTT_USER'] , password=os.environ['MQTT_PW'] )
print("Starting MQTT client connection to " + os.environ['MQTT_HOST'] )
mqttclient.enable_logger()

# MQTT connect is a blocking call
mqttclient.connect(os.environ['MQTT_HOST'],os.environ['PORT'],60)
print("MQTT client connected")
mqttclient.loop_start()
mqttclient.publish(sys.argv[0],"Connected")

# Bring up MongoDB changestream
print("Starting MongoClient")
client=pymongo.MongoClient(os.environ['CHANGE_STREAM_DB'])
try:
    client.admin.command('ping')
    print("MongoClient connected")
    logger.info("Connected to Mongo")
except ConnectionFailure:
    print("MongoDB server unavailable")
    logger.error("Could not connect to Mongo")

#
# Watch the stream of changes
# on each change, grab the provided resume_token
#
try:
    resume_token = None
    option={ 'full_document':'updateLookup' }
    with client.makerauth.watch([], **option) as change_stream:
        for update_change in change_stream:
            resume_token = change_stream.resume_token
            process_mongo_update(update_change)

except pymongo.errors.PyMongoError:
    if resume_token is None:
       logging.error('...')
    else:
        print("Resuming MongoClient")
        logger.warning("Resuming MongoClient")
        option={ 'full_document':'updateLookup',resume_after:resume_token }
        with client.makerauth.watch([], **option) as change_stream:
            for update_change in stream:
                process_mongo_update(update_change)

client.close()
mqttclient.disconnect()
logger.error("Exiting!")
time.sleep(11)
exit(2)
