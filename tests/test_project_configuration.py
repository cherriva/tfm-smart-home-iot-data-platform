import json
import os
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).parents[1]


class ProjectConfigurationTests(unittest.TestCase):
    def test_matter_inventory_is_not_ignored(self):
        result = subprocess.run(
            ["git", "check-ignore", "matter_inventory.csv"],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)

    def test_removed_integrations_and_anomaly_pipeline_are_absent(self):
        forbidden = [
            "homekit", "sonoff", "anomaly", "ANOMALY",
            "SEMANA_1.md", "SEMANA_3.md",
        ]
        roots = [ROOT / "README.md", ROOT / ".env.example", ROOT / "docker-compose.yml"]
        roots.extend((ROOT / "docs").glob("*.md"))
        for path in roots:
            content = path.read_text(encoding="utf-8")
            for term in forbidden:
                self.assertNotIn(term, content, f"Referencia obsoleta en {path}: {term}")

        self.assertFalse((ROOT / "airflow" / "dags" / "anomaly_detection.py").exists())
        self.assertFalse((ROOT / "airflow" / "jobs" / "anomaly_pipeline.py").exists())

    def test_dashboards_are_valid_json(self):
        for path in (ROOT / "grafana" / "dashboards").glob("*/*.json"):
            dashboard = json.loads(path.read_text(encoding="utf-8"))
            self.assertTrue(dashboard.get("uid"), path)
            self.assertIsInstance(dashboard.get("panels"), list)

    def test_compose_declares_core_services(self):
        environment = os.environ.copy()
        environment.setdefault("MQTT_PASSWORD", "test-only")
        result = subprocess.run(
            ["docker", "compose", "config", "--services"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            env=environment,
        )
        if result.returncode != 0:
            self.skipTest("Docker Compose no está disponible")
        services = set(result.stdout.splitlines())
        self.assertTrue({"redpanda", "mqtt-ingestor", "synthetic-generator", "spark-lakehouse", "trino", "grafana"} <= services)
        self.assertNotIn("anomaly_detection", services)


if __name__ == "__main__":
    unittest.main()
