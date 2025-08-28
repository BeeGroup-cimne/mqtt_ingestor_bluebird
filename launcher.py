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
        producer = beelib.beekafka.create_kafka_producer(config['kafka']['connection'], encoding="JSON")
        # Generar la uri i guardar les dades a HBASE
        logger.debug(f"Received message: {msg.topic}")
        data = json.loads(msg.payload.decode())
        df = pd.DataFrame(data)
        if df.empty:
            return
        df['uri'] = df.apply(create_uri, axis=1)
        df = df.dropna(subset=['uri'])
        df = df.drop_duplicates(subset=['uri'])
        df['_time'] = pd.to_datetime(df["_time"]).astype(int) // 10**9
        df = df[["uri", "_time", "_value"]]
        beelib.beekafka.send_to_kafka(producer, config['kafka']['topic'], None,
                                      df.to_dict(orient='records'),
                                      tables=["bluebird:static_montcada_"], row_keys=[["uri", "_time"]])
        producer.close()

    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.username_pw_set(config['mqtt']['connection']['username'], config['mqtt']['connection']['password'])
    context = ssl.create_default_context()
    mqtt_client.tls_set_context(context)
    mqtt_client.connect(**config['mqtt']['connection']["server"])
    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    mqtt_client.loop_forever()


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
