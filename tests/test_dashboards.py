import json
from pathlib import Path
import unittest


ROOT = Path(__file__).parents[1]
DASHBOARD_ROOT = ROOT / "grafana" / "dashboards"


class DashboardContractTests(unittest.TestCase):
    def test_expected_dashboard_catalog(self):
        dashboards = {
            json.loads(path.read_text(encoding="utf-8"))["uid"]
            for path in DASHBOARD_ROOT.glob("*/*.json")
        }
        self.assertEqual(
            dashboards,
            {
                "tfm-home-overview",
                "tfm-comfort",
                "tfm-energy",
                "tfm-occupancy",
                "tfm-air-quality",
                "tfm-security",
                "tfm-water",
                "tfm-hvac",
                "tfm-garage",
                "tfm-data-quality",
                "tfm-ingestion",
            },
        )

    def test_panels_only_query_gold_models(self):
        forbidden = ("tfm.iot_silver.", "tfm.iot_bronze.", "tfm.iot_quality.")
        for path in DASHBOARD_ROOT.glob("*/*.json"):
            dashboard = json.loads(path.read_text(encoding="utf-8"))
            for panel in dashboard["panels"]:
                for query in panel.get("targets", []):
                    sql = query.get("rawSQL", "")
                    self.assertTrue(sql, f"Consulta vacía en {path.name}: {panel['title']}")
                    self.assertFalse(
                        any(schema in sql for schema in forbidden),
                        f"El panel {path.name}/{panel['title']} evita la capa Gold",
                    )


if __name__ == "__main__":
    unittest.main()
