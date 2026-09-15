import json
import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'airflow/jobs'))
from extract_aemet import AemetError, extract_daily_weather, load_silver_iceberg, transform_silver, validate_payload

DAY = date(2026, 9, 9)


class AemetTests(unittest.TestCase):
    def test_latin1_original_payload(self):
        payload = json.dumps([{'fecha': str(DAY), 'indicativo': '1', 'nombre': 'Málaga'}], ensure_ascii=False).encode('latin-1')
        self.assertEqual(validate_payload(payload, DAY)[0]['nombre'], 'Málaga')

    def test_utf8_station_name_is_preserved_in_silver(self):
        payload = json.dumps([{'fecha': str(DAY), 'indicativo': '9263D', 'nombre': 'Málaga', 'tmed': '12,3'}], ensure_ascii=False).encode()
        record = transform_silver(payload)[0]
        self.assertEqual(record['station_name'], 'Málaga')
        self.assertEqual(record['mean_temperature_c'], 12.3)

    @patch('extract_aemet.execute_trino')
    def test_iceberg_load_uses_idempotent_merge(self, execute):
        result = load_silver_iceberg([{
            'source': 'aemet', 'station_id': '9263D', 'station_name': "O'Hare", 'province': 'Navarra',
            'altitude_m': 100.0, 'observation_date': str(DAY), 'mean_temperature_c': 12.3,
            'min_temperature_c': None, 'max_temperature_c': None, 'precipitation_mm': None,
            'mean_humidity_pct': None, 'min_humidity_pct': None, 'max_humidity_pct': None,
            'mean_wind_speed_kmh': None, 'max_gust_kmh': None, 'sunshine_hours': None,
            'loaded_at': '2026-09-10T00:00:00+00:00',
        }])
        self.assertEqual(result['records'], 1)
        statement = execute.call_args.args[0]
        self.assertIn('MERGE INTO tfm.iot_silver.aemet_daily', statement)
        self.assertIn("O''Hare", statement)

    def test_invalid_datasets_rejected(self):
        for rows in [[], {}, [{'fecha': '2026-09-08', 'indicativo': '1'}], [{'fecha': str(DAY)}],
                     [{'fecha': str(DAY), 'indicativo': '1'}] * 2]:
            with self.subTest(rows=rows), self.assertRaises(AemetError):
                validate_payload(json.dumps(rows).encode(), DAY)

    @patch('extract_aemet.requests.get')
    def test_two_stage_download_and_header_auth(self, get):
        payload = json.dumps([{'fecha': str(DAY), 'indicativo': '1'}]).encode()
        get.side_effect = [Mock(status_code=200, json=lambda: {'estado': 200, 'datos': 'https://opendata.aemet.es/data/test'}),
                           Mock(status_code=200, content=payload)]
        self.assertEqual(extract_daily_weather('secret', DAY), payload)
        self.assertIn('/valores/climatologicos/', get.call_args_list[0].args[0])
        self.assertEqual(get.call_args_list[0].kwargs['headers'], {'api_key': 'secret'})
        self.assertIsNone(get.call_args_list[1].kwargs['headers'])

    @patch('extract_aemet.requests.get')
    def test_missing_data_not_reported_as_success(self, get):
        get.return_value = Mock(status_code=200, json=lambda: {'estado': 404})
        with self.assertRaises(AemetError):
            extract_daily_weather('secret', DAY)
        self.assertEqual(get.call_count, 1)

    @patch('extract_aemet.requests.get')
    def test_unexpected_download_host_rejected(self, get):
        get.return_value = Mock(status_code=200, json=lambda: {'estado': 200, 'datos': 'https://example.com/data'})
        with self.assertRaises(AemetError):
            extract_daily_weather('secret', DAY)
        self.assertEqual(get.call_count, 1)


if __name__ == '__main__':
    unittest.main()
