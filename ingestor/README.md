# MQTT Ingestor (BlueBird)

Continuous service that subscribes to MQTT topics from IoT devices, converts each
message into BlueBird measurement URIs, and publishes it to Kafka so that
`raw_store` can persist it to Cassandra.

```
MQTT broker ──TLS/WSS──▶ ingestor ──▶ Kafka ──▶ raw_store ──▶ Cassandra
```

The MQTT client uses WebSockets (`transport="websockets"`, path `/mqtt`) with TLS.
Messages are processed one by one, not in batches. `loop_forever()` keeps the
connection alive and reconnects automatically.

## Message flow

1. `on_connect` subscribes to `mqtt.topic`.
2. `on_message` parses the JSON payload into a DataFrame.
3. `create_uri()` builds one semantic URI per record.
4. `_time` is converted to epoch seconds as `ts`; `_value` becomes `value`.
5. Rows are deduplicated by `id`, then sent to `kafka.topic` with the Cassandra
   table and row keys from the configuration.
6. A Kafka producer is created and closed for each message. Errors are logged;
   the service keeps running.

The output columns sent to Kafka are always `id`, `ts`, `value`.

## URI generation

The device type is selected by the first field present in the record:

| Field      | Type      | Building | URI                                                                      |
| ---------- | --------- | -------- | ------------------------------------------------------------------------ |
| `Circutor` | `circutor`| 1        | `.../data#measurement-influx-1-circutor-{device}-{field}-PT1H`           |
| `Sala`     | `sala`    | 1        | `.../data#measurement-influx-1-sala-{device}-{field}-PT1H`               |
| `sensor`   | `sensor`  | 2        | `.../data#measurement-influx-2-sensor-{device}-{field}-PT1H`             |

Namespace: `https://bluebird-project.eu/data#`. Device and field are slugified
(lowercase, special characters removed).

Records with no recognised type get `id = None` and are dropped. Any exception
during URI generation produces `id = "error"`.

## Configuration

The file is read from `--config/-c`, or from `CONF_FILE` when running in
Kubernetes with the secret mounted at `/app/config.json`.

```json
{
  "mqtt": {
    "connection": {
      "server": { "host": "mqtt.example.com", "port": 443 },
      "username": "...",
      "password": "..."
    },
    "topic": "bluebird/#"
  },
  "kafka": {
    "connection": { "host": "kafka1.internal", "port": "9092" },
    "topic": "hbase.bluebird"
  },
  "cassandra": {
    "connection": {
      "contact_points": ["bluebird-dc1-service.bluebird"],
      "port": 9042,
      "local_dc": "dc1"
    },
    "auth": { "username": "...", "password": "...", "active": "false" },
    "montcada": {
      "table": "bluebird:static_montcada_",
      "row_keys": [["id", "ts"]]
    }
  }
}
```

`row_keys` must match the output columns (`id`, `ts`).

## Run locally

```bash
cd ingestor
pip install -r requirements.txt
python launcher.py -c ../config.json
```

Initial connection is retried 5 times, 4 seconds apart.

## Build and deploy

```bash
docker buildx build --platform linux/amd64 --push \
  -t 1l41bgc7.c1.gra9.container-registry.ovh.net/bluebird/mqtt_ingestor:latest .
```

Kubernetes uses a `Deployment` (not a CronJob) with 1 replica and
`strategy: Recreate`, so two instances never subscribe to the same topic:

```bash
kubectl create secret generic mqtt-ingestor-secret-cassandra \
  --from-file=config.json=config.json -n bluebird
kubectl apply -f kubeconfig/deployment.yaml
```

Namespace: `bluebird`. Secret mounted at `/app/config.json`.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `Connection error` on startup | Broker unreachable, TLS misconfigured, or bad credentials. |
| Empty DataFrame | Invalid JSON or missing `Circutor`/`Sala`/`sensor`, `_field`, `_value`, or `_time`. |
| `Unknown type` in logs | No recognised device field; record is ignored. |
| `id = "error"` | `create_uri` exception, usually an unhandleable slug. |
| Kafka timeout | High message volume; increase the producer's `request_timeout_ms`. |

There is no historical recalculation: messages are real-time only, and anything
not received cannot be recovered (unless the broker retains it).

## License

Licensed under the [European Union Public Licence v1.2](../LICENSE).

## Contributors
