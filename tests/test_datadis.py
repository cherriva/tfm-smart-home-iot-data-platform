import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'airflow/jobs'))
from extract_datadis import DatadisError, months_ending, transform


class DatadisTests(unittest.TestCase):
    def test_month_window_crosses_year(self):
        self.assertEqual(months_ending(date(2026, 1, 3), 2), ['2025/12', '2026/01'])

    def test_transform_preserves_hourly_energy_fields(self):
        rows = transform([{'cups': 'ES123', 'date': '2026/09/01', 'time': '01', 'consumptionKWh': '0.45',
                           'obtainMethod': 'Real', 'surplusEnergyKWh': None, 'generationEnergyKWh': '0.1',
                           'selfConsumptionEnergyKWh': '0.1'}], {'cups': 'ES123', 'distributorCode': '2'})
        self.assertEqual(rows[0]['reading_date'], '2026-09-01')
        self.assertEqual(rows[0]['consumption_kwh'], 0.45)
        self.assertEqual(rows[0]['generation_energy_kwh'], 0.1)

    def test_transform_rejects_missing_period(self):
        with self.assertRaises(DatadisError):
            transform([{'date': '2026/09/01'}], {'cups': 'ES123', 'distributorCode': '2'})
