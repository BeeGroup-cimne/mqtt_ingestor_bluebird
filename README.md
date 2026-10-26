# MQTT Ingestor — BlueBird

Continuous service that subscribes to MQTT topics from Bluebird IoT devices,
converts each message into Bluebird measurement URIs, and publishes it to Kafka
so `raw_store` can persist it to Cassandra.

```
MQTT broker ──TLS/WSS──▶ ingestor ──▶ Kafka (hbase.bluebird) ──▶ raw_store ──▶ Cassandra
```

The MQTT client uses WebSockets with TLS, messages are processed one by one,
and `loop_forever()` keeps the connection alive with automatic reconnect.
Runs as a Deployment in the `bluebird` namespace.

Full documentation (message flow, URI generation, deployment) is in
[`ingestor/README.md`](ingestor/README.md).

## License

This project is licensed under the European Union Public Licence v. 1.2 (EUPL-1.2),
see the [LICENSE](LICENSE) file.

## Contributors

- Hamza Boulhani (hboulhani@cimne.upc.edu)
