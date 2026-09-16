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


if __name__ == "__main__":
    unittest.main()
