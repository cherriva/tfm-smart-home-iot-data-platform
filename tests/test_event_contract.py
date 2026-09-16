import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark-lakehouse"))
from event_contract import QualityConfig, normalize, parse_timestamp

NOW = datetime(2026, 9, 13, 12, tzinfo=timezone.utc)


def event(**changes):
    payload = dict(source="matter", entity_id="sensor.test_temperature", domain="sensor", state="23.4",
                   event_timestamp=NOW.isoformat(), updated_timestamp=NOW.isoformat(),
                   ingestion_timestamp=NOW.isoformat(),
                   attributes={"device_class": "temperature", "unit_of_measurement": "°C"})
    payload.update(changes)
    return payload


def check(payload=None, broker=NOW, config=None):
    return normalize(json.dumps(payload if payload is not None else event()), broker, config)


class ContractTests(unittest.TestCase):
    def test_valid(self):
        row = check()
        self.assertEqual(row["quality_errors"], [])
        self.assertEqual(row["numeric_state"], 23.4)
        self.assertEqual(row["event_time"], NOW)

    def test_malformed_json(self):
        for raw in ['{"state":', '{"state":NaN}', '{"state":Infinity}', '']:
            with self.subTest(raw=raw):
                self.assertIn("invalid_json", normalize(raw, NOW)["quality_errors"])

    def test_non_object(self):
        for payload in [[], None, 42, "string"]:
            self.assertIn("invalid_payload_type", normalize(json.dumps(payload), NOW)["quality_errors"])

    def test_missing_fields_all_causes(self):
        errors = check({})["quality_errors"]
        self.assertTrue(set(["missing_source", "missing_entity", "missing_state", "missing_event_time"]).issubset(errors))

    def test_unknown_unavailable(self):
        for state in ["unknown", "unavailable", "UNAVAILABLE"]:
            row = check(event(state=state))
            self.assertEqual(row["quality_status"], "quarantined")
            self.assertIn("state_" + state.lower(), row["quality_errors"])

    def test_timestamp_timezone(self):
        self.assertEqual(parse_timestamp("2026-09-13T14:00:00+02:00"), NOW)
        self.assertEqual(parse_timestamp("2026-09-13T12:00:00Z"), NOW)
        for value in ["2026-09-13", "2026-09-13T12:00:00", "yesterday", 123, "2026-99-13T12:00:00Z"]:
            self.assertIsNone(parse_timestamp(value))

    def test_invalid_timestamp_never_falls_back(self):
        row = check(event(event_timestamp="bad"))
        self.assertIsNone(row["event_time"])
        self.assertIn("invalid_event_time", row["quality_errors"])

    def test_optional_bad_timestamp(self):
        self.assertIn("invalid_updated_time", check(event(updated_timestamp="bad"))["quality_errors"])

    def test_numeric_conversion(self):
        for state, expected in [("2.34e1", 23.4), (" 23.4 ", 23.4), (23.4, 23.4), ("+.5", .5), ("-1.5", -1.5)]:
            self.assertEqual(check(event(state=state))["numeric_state"], expected)

    def test_non_finite_and_bad_numbers(self):
        for state in ["NaN", "Infinity", "-inf", "1e9999", "23,4", "abc"]:
            self.assertEqual(check(event(state=state))["quality_status"], "quarantined")

    def test_non_numeric_actuator_is_valid(self):
        row = check(event(entity_id="light.demo", domain="light", state="on", attributes={}))
        self.assertEqual(row["quality_errors"], [])
        self.assertIsNone(row["numeric_state"])

    def test_temperature_bounds_configurable(self):
        config = QualityConfig(temperature_min_c=0, temperature_max_c=40)
        for state in ["0", "40"]:
            self.assertEqual(check(event(state=state), config=config)["quality_errors"], [])
        for state in ["-0.1", "40.1"]:
            self.assertIn("temperature_out_of_range", check(event(state=state), config=config)["quality_errors"])

    def test_temperature_units(self):
        for state, unit in [("73.4", "°F"), ("296.55", "K")]:
            self.assertEqual(check(event(state=state, attributes={"device_class": "temperature", "unit_of_measurement": unit}))["quality_errors"], [])
        self.assertIn("unsupported_temperature_unit", check(event(attributes={"device_class": "temperature"}))["quality_errors"])

    def test_humidity_bounds(self):
        for state in ["0", "100"]:
            self.assertEqual(check(event(state=state, attributes={"device_class": "humidity", "unit_of_measurement": "%"}))["quality_errors"], [])
        for state in ["-1", "101"]:
            self.assertIn("humidity_out_of_range", check(event(state=state, attributes={"device_class": "humidity", "unit_of_measurement": "%"}))["quality_errors"])

    def test_future_boundary(self):
        self.assertEqual(check(event(event_timestamp=(NOW + timedelta(seconds=300)).isoformat()))["quality_errors"], [])
        self.assertIn("future_event", check(event(event_timestamp=(NOW + timedelta(seconds=301)).isoformat()))["quality_errors"])

    def test_historical_and_out_of_order_valid(self):
        row = check(event(event_timestamp=(NOW - timedelta(days=365)).isoformat()))
        self.assertEqual(row["quality_errors"], [])
        self.assertEqual(row["quality_warnings"], ["late_event"])

    def test_duplicate_identity_ignores_transport(self):
        one = check()
        two = check(event(ingestion_timestamp=(NOW + timedelta(minutes=1)).isoformat(),
                          bridge_timestamp=(NOW + timedelta(minutes=2)).isoformat(), mqtt_topic="other",
                          kafka_offset=999, event_id="untrusted-id"))
        self.assertEqual(one["event_id"], two["event_id"])

    def test_identity_normalizes_timezone_and_key_order(self):
        one = check()
        two = check(event(event_timestamp="2026-09-13T14:00:00+02:00", updated_timestamp="2026-09-13T12:00:00Z",
                          attributes={"unit_of_measurement": "°C", "device_class": "temperature"}))
        self.assertEqual(one["event_id"], two["event_id"])

    def test_real_changes_different_identity(self):
        for changed in [event(source="synthetic"), event(state="25"),
                        event(updated_timestamp=(NOW + timedelta(seconds=1)).isoformat()),
                        event(attributes={"device_class": "temperature", "unit_of_measurement": "°C", "new": 1})]:
            self.assertNotEqual(check()["event_id"], check(changed)["event_id"])

    def test_nested_attributes_preserved(self):
        row = check(event(attributes={"nested": {"a": [1, True]}, "synthetic": True}))
        self.assertEqual(json.loads(row["attributes_json"])["nested"], {"a": [1, True]})
        self.assertEqual(row["attributes"]["synthetic"], "true")

    def test_schema_and_domain_errors(self):
        self.assertIn("domain_mismatch", check(event(domain="light"))["quality_errors"])
        self.assertIn("unsupported_schema_version", check(event(schema_version=2))["quality_errors"])
        self.assertIn("invalid_attributes", check(event(attributes=[]))["quality_errors"])

    def test_invalid_config_fails_fast(self):
        for params in [{"temperature_min_c": 90}, {"inactivity_seconds": -1}, {"temperature_max_c": float("nan")}]:
            with self.assertRaises(ValueError):
                QualityConfig(**params)


if __name__ == "__main__":
    unittest.main()
