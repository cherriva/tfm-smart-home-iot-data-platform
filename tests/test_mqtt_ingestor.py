import importlib.util
import json
import os
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch


ROOT = Path(__file__).parents[1]
MODULE_PATH = ROOT / "mqtt-ingestor" / "main.py"


class Message:
    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload


def load_module():
    mqtt = types.ModuleType("paho.mqtt.client")
    mqtt.MQTT_ERR_SUCCESS = 0
    mqtt.MQTTv5 = 5
    mqtt.MQTT_CLIENT = object
    mqtt.CallbackAPIVersion = types.SimpleNamespace(VERSION2=2)
    mqtt.Client = Mock
    paho = types.ModuleType("paho")
    paho.mqtt = types.SimpleNamespace(client=mqtt)
    kafka = types.ModuleType("confluent_kafka")
    kafka.Producer = Mock
    with patch.dict(sys.modules, {
        "paho": paho,
        "paho.mqtt": paho.mqtt,
        "paho.mqtt.client": mqtt,
        "confluent_kafka": kafka,
    }), patch.dict(os.environ, {"MQTT_HOST": "test-broker"}):
        spec = importlib.util.spec_from_file_location("mqtt_ingestor_under_test", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module


class MqttIngestorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_module()

    def setUp(self):
        self.module.producer = Mock()

    def produced_payload(self):
        return json.loads(self.module.producer.produce.call_args.kwargs["value"])

    def test_matter_message_is_normalized_and_published(self):
        message = Message(
            "tfm/matter/events/sensor/Temperatura Salón",
            json.dumps({
                "source": "anything-from-home-assistant",
                "entity_id": "sensor.Temperatura Salón",
                "domain": "sensor",
                "state": "23.4",
                "attributes": {"device_class": "temperature"},
            }).encode(),
        )

        self.module.on_message(None, None, message)

        record = self.produced_payload()
        self.assertEqual(record["source"], "matter")
        self.assertEqual(record["entity_id"], "sensor.temperatura_salon")
        self.assertEqual(record["domain"], "sensor")
        self.assertEqual(record["attributes"]["original_entity_id"], "sensor.Temperatura Salón")
        self.assertEqual(self.module.producer.produce.call_args.args[0], "tfm.matter.events")

    def test_invalid_json_is_forwarded_unchanged_for_quarantine(self):
        raw = b"{not-json"
        self.module.on_message(None, None, Message("tfm/matter/events/sensor/test", raw))

        call = self.module.producer.produce.call_args
        self.assertEqual(call.kwargs["value"], raw)
        self.assertEqual(call.kwargs["key"], b"tfm/matter/events/sensor/test")


if __name__ == "__main__":
    unittest.main()
