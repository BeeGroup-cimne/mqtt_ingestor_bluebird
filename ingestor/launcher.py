import json
import time
import argparse
import dotenv
import os
import ssl
import numpy as np
import pytz
import redis
import pandas as pd
from datetime import datetime, timezone
import beelib
import pickle

from slugify import slugify

from project_logger import get_logger
import urllib.parse
import paho.mqtt.client as mqtt

logger = get_logger("MQTTIngestor")

NAMESPACE = "https://bluebird-project.eu/data#"


def create_uri(x):
    try:
        if "Circutor" in x and pd.notna(x["Circutor"]):
            id_building = 1
            stype = "circutor"
            sid = slugify(x.Circutor)
        elif "Sala" in x and pd.notna(x["Sala"]):
            id_building = 1
            stype = "sala"
            sid = slugify(x.Sala)
        elif "sensor" in x and pd.notna(x["sensor"]):
            id_building = 2
            stype = "sensor"
            sid = slugify(x.sensor)
        else:
            print(f"Unknown type: {x}")
            return None
        s_field = slugify(x._field)
        return f"{NAMESPACE}measurement-influx-{id_building}-{stype}-{sid}-{s_field}-PT1H"
    except:
        return "error"


def start_collecting(config=None):
    config = beelib.beeconfig.read_config(config)
    # The callback for when the client receives a CONNACK response from the server.

    def on_connect(client, userdata, flags, reason_code, properties):
        logger.info(f"Connected with result code {reason_code}")
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe(config['mqtt']['topic'])

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        producer = None
        try:
            logger.info(f"Received message: topic={msg.topic} bytes={len(msg.payload)}")
            data = json.loads(msg.payload.decode())
            df = pd.DataFrame(data)
            if df.empty:
                logger.warning(f"Empty payload on {msg.topic}")
                return
            n_in = len(df)
            df['id'] = df.apply(create_uri, axis=1)
            df = df.dropna(subset=['id'])
            n_bad = int((df['id'] == "error").sum())
            df = df.drop_duplicates(subset=['id'])
            df['ts'] = pd.to_datetime(df["_time"]).astype(int) // 10**9
            df = df.rename(columns={"_value": "value"})[["id", "ts", "value"]]
            producer = beelib.beekafka.create_kafka_producer(config['kafka']['connection'], encoding="JSON")
            beelib.beekafka.send_to_kafka(producer, config['kafka']['topic'], None,
                                          df.to_dict(orient='records'),
                                          tables=[config["cassandra"]["montcada"]["table"]],
                                          row_keys=config["cassandra"]["montcada"]["row_keys"])
            logger.info(f"Sent to Kafka: rows_in={n_in} rows_out={len(df)} bad_uri={n_bad} "
                        f"ts_min={df['ts'].min()} ts_max={df['ts'].max()}")
        except Exception:
            logger.exception(f"Error processing message on {msg.topic}")
        finally:
            if producer is not None:
                producer.close()

    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2 , transport="websockets")
    mqtt_client.ws_set_options(path="/mqtt")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.username_pw_set(config['mqtt']['connection']['username'], config['mqtt']['connection']['password'])
    mqtt_client.tls_set_context(ssl.create_default_context())
    mqtt_client.connect(**config['mqtt']['connection']["server"], keepalive = 30)
    # Process network traffic, dispatch callbacks and handle reconnecting on
    # every iteration. The work loop is the heartbeat source: beat() must run
    # here (not in a side thread) so a hung loop stops the heartbeat.
    while True:
        beelib.beeheartbeat.beat()
        mqtt_client.loop(timeout=10)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description='Gathers MQTT data')
    ap.add_argument('--config', '-c', help='Configuration file', required=False, default=None)

    logger.debug("starting ingestor")
    dotenv.load_dotenv()
    if os.getenv("PYCHARM_HOSTED") is not None:
        args = ap.parse_args([])
    else:
        args = ap.parse_args()
        for attempt in range(5):
            try:
                start_collecting(**args.__dict__)
                break
            except Exception as e:
                logger.debug(f"connecition error: {e}")
                time.sleep(4)
