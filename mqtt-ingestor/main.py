"""Bridge MQTT -> Redpanda/Kafka for the TFM Home Assistant telemetry stream."""

import json
import logging
import os
import ssl
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from confluent_kafka import Producer


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("tfm-mqtt-ingestor")


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


MQTT_HOST = os.environ["MQTT_HOST"]
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "tfm/+/events/#")
MQTT_TLS = env_bool("MQTT_TLS", True)
MQTT_TLS_INSECURE = env_bool("MQTT_TLS_INSECURE", False)
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "tfm-mqtt-ingestor")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tfm.matter.events")


producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "tfm-mqtt-ingestor",
        "enable.idempotence": True,
        "acks": "all",
    }
)


def delivery_report(error, message) -> None:
    if error is not None:
        LOGGER.error("Error entregando mensaje a Redpanda: %s", error)
        return
    LOGGER.debug(
        "Mensaje entregado topic=%s partition=%s offset=%s",
        message.topic(),
        message.partition(),
        message.offset(),
    )


def on_connect(client, userdata, flags, reason_code, properties) -> None:
    if reason_code != 0:
        LOGGER.error("No se pudo conectar a MQTT: %s", reason_code)
        return
    result, _ = client.subscribe(MQTT_TOPIC, qos=1)
    if result != mqtt.MQTT_ERR_SUCCESS:
        LOGGER.error("No se pudo suscribir a %s: %s", MQTT_TOPIC, result)
        return
    LOGGER.info("Suscrito a MQTT %s:%s topic=%s", MQTT_HOST, MQTT_PORT, MQTT_TOPIC)
    LOGGER.info("Publicando en Redpanda %s topic=%s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties) -> None:
    LOGGER.warning("Desconectado de MQTT: %s", reason_code)


def on_message(client, userdata, message) -> None:
    try:
        event = json.loads(message.payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        event = None

    if isinstance(event, dict):
        record = dict(event)
        record["mqtt_topic"] = message.topic
        record["bridge_timestamp"] = datetime.now(timezone.utc).isoformat()
        key = str(record.get("entity_id", message.topic))
        payload = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    else:
        # Preserve malformed/non-object payloads for Bronze and explicit quarantine.
        LOGGER.warning("Payload no válido; se conserva para cuarentena")
        key, payload = message.topic, message.payload

    while True:
        try:
            producer.produce(KAFKA_TOPIC, key=key.encode("utf-8"), value=payload,
                             on_delivery=delivery_report)
            producer.poll(0)
            LOGGER.info("Evento recibido y encolado key=%s", key)
            break
        except BufferError:
            LOGGER.warning("Cola de Redpanda llena; esperando espacio")
            producer.poll(1)


def build_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
        protocol=mqtt.MQTTv5,
    )
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    if MQTT_TLS:
        client.tls_set_context(ssl.create_default_context())
        client.tls_insecure_set(MQTT_TLS_INSECURE)

    client.reconnect_delay_set(min_delay=1, max_delay=30)
    return client


def main() -> None:
    client = build_mqtt_client()
    while True:
        try:
            LOGGER.info("Conectando a MQTT %s:%s TLS=%s", MQTT_HOST, MQTT_PORT, MQTT_TLS)
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_forever()
        except KeyboardInterrupt:
            break
        except Exception as exc:  # noqa: BLE001 - keep the bridge alive on transient failures
            LOGGER.error("Error del ingestor: %s; reintentando en 5 s", exc)
            time.sleep(5)
        finally:
            producer.flush(5)


if __name__ == "__main__":
    main()
