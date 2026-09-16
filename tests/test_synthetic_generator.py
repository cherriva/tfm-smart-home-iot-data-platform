import importlib.util
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch


MODULE_PATH = Path(__file__).parents[1] / "synthetic-generator" / "main.py"
SPEC = importlib.util.spec_from_file_location("synthetic_generator", MODULE_PATH)
synthetic_generator = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
with patch.dict(sys.modules, {"confluent_kafka": Mock(Producer=Mock())}):
    SPEC.loader.exec_module(synthetic_generator)


class SyntheticGeneratorTests(unittest.TestCase):
    def test_entity_without_embedded_domain_uses_inventory_domain(self):
        self.assertEqual(
            synthetic_generator.synthetic_entity_id("temperatura_salon", "sensor"),
            "sensor.synthetic_temperatura_salon",
        )

    def test_entity_with_embedded_domain_keeps_valid_shape(self):
        self.assertEqual(
            synthetic_generator.synthetic_entity_id("light.lampara_salon", "light"),
            "light.synthetic_lampara_salon",
        )

    def test_missing_domain_is_rejected(self):
        with self.assertRaises(ValueError):
            synthetic_generator.synthetic_entity_id("temperatura_salon")

    def test_default_bootstrap_window_is_forty_days(self):
        if "SYNTHETIC_BOOTSTRAP_HOURS" not in os.environ:
            self.assertEqual(synthetic_generator.BOOTSTRAP_HOURS, 24 * 40)

    def test_generated_event_is_matter_compatible_and_reproducible(self):
        row = {
            "entity_id": "sensor.temperature_test",
            "domain": "sensor",
            "device_class": "temperature",
            "unit_of_measurement": "°C",
            "current_state": "23",
            "area_id": "test_room",
            "device_name": "Test temperature",
        }
        timestamp = synthetic_generator.datetime(2026, 9, 16, tzinfo=synthetic_generator.timezone.utc)
        _, first = synthetic_generator.build_event(row, timestamp)
        _, second = synthetic_generator.build_event(row, timestamp)
        self.assertEqual(first, second)
        self.assertEqual(first["source"], "synthetic")
        self.assertEqual(first["entity_id"], "sensor.synthetic_temperature_test")
        self.assertNotIn("is_anomaly_expected", first["attributes"])


if __name__ == "__main__":
    unittest.main()
