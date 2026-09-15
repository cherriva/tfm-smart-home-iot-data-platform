"""Datadis V2 hourly electricity consumption -> MinIO Bronze and Iceberg Silver."""
import argparse
import hashlib
import json
import logging
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path

import boto3
import requests
from botocore.config import Config
from dotenv import load_dotenv

from extract_aemet import _sql_value, execute_trino

BASE_URL = 'https://datadis.es'
API_BASE = BASE_URL + '/api-private/api'
LOG = logging.getLogger(__name__)


class DatadisError(RuntimeError):
    pass


def request(method, url, **kwargs):
    """Datadis may incorrectly label plain responses as gzip; request identity encoding."""
    headers = {'Accept-Encoding': 'identity', **kwargs.pop('headers', {})}
    for attempt in range(3):
        try:
            response = requests.request(method, url, headers=headers, timeout=60, **kwargs)
        except requests.RequestException:
            raise DatadisError('Datadis: fallo de conexión o timeout') from None
        if response.status_code == 429 and attempt < 2:
            try:
                delay = min(max(int(response.headers.get('Retry-After', '5')), 1), 60)
            except ValueError:
                delay = 5
            time.sleep(delay)
            continue
        if response.status_code != 200:
            raise DatadisError(f'Datadis HTTP {response.status_code}')
        return response
    raise DatadisError('Datadis HTTP 429')


def login() -> str:
    username, password = os.getenv('DATADIS_USERNAME'), os.getenv('DATADIS_PASSWORD')
    if not username or not password:
        raise DatadisError('Faltan DATADIS_USERNAME o DATADIS_PASSWORD')
    # The API returns a plain-text token. Never log it or expose it in an exception.
    token = request('POST', BASE_URL + '/nikola-auth/tokens/login',
                    data={'username': username, 'password': password}).text.strip()
    if not token:
        raise DatadisError('Datadis: autenticación sin token')
    return token


def api_get(endpoint: str, token: str, params=None):
    response = request('GET', API_BASE + endpoint,
                       headers={'Authorization': f'Bearer {token}'}, params=params or {})
    try:
        return response.json()
    except ValueError:
        raise DatadisError('Datadis: respuesta JSON no válida') from None


def month_string(value: date) -> str:
    return value.strftime('%Y/%m')


def months_ending(value: date, count: int) -> list[str]:
    if count < 1:
        raise ValueError('DATADIS_LOOKBACK_MONTHS debe ser positivo')
    year, month = value.year, value.month
    result = []
    for _ in range(count):
        result.append(f'{year:04d}/{month:02d}')
        year, month = (year - 1, 12) if month == 1 else (year, month - 1)
    return list(reversed(result))


def supplies(token: str) -> list[dict]:
    payload = api_get('/get-supplies-v2', token)
    rows = payload.get('supplies', []) if isinstance(payload, dict) else []
    if not isinstance(rows, list) or not rows:
        raise DatadisError('Datadis: no se encontraron suministros')
    valid = [row for row in rows if isinstance(row, dict) and row.get('cups') and row.get('distributorCode')]
    if not valid:
        raise DatadisError('Datadis: suministros sin CUPS o distribuidora')
    return valid


def consumption(token: str, supply: dict, start_month: str, end_month: str) -> list[dict]:
    params = {'cups': supply['cups'], 'distributorCode': supply['distributorCode'],
              'startDate': start_month, 'endDate': end_month, 'measurementType': 0}
    if supply.get('pointType') is not None:
        params['pointType'] = supply['pointType']
    payload = api_get('/get-consumption-data-v2', token, params)
    rows = payload.get('timeCurve', []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        raise DatadisError('Datadis: curva de consumo inválida')
    return rows


def number(value):
    if value in (None, ''):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def date_value(value) -> date:
    if not isinstance(value, str):
        raise DatadisError('Datadis: fecha de lectura inválida')
    try:
        return date.fromisoformat(value.replace('/', '-'))
    except ValueError:
        raise DatadisError('Datadis: fecha de lectura inválida') from None


def transform(rows: list[dict], supply: dict) -> list[dict]:
    output, now = [], datetime.now(timezone.utc).isoformat()
    for row in rows:
        if not isinstance(row, dict) or row.get('date') is None or row.get('time') is None:
            raise DatadisError('Datadis: lectura sin fecha o periodo')
        output.append({
            'cups': str(row.get('cups') or supply['cups']),
            'distributor_code': str(supply['distributorCode']),
            'reading_date': date_value(row['date']).isoformat(),
            'period': str(row['time']),
            'consumption_kwh': number(row.get('consumptionKWh')),
            'obtain_method': row.get('obtainMethod'),
            'surplus_energy_kwh': number(row.get('surplusEnergyKWh')),
            'generation_energy_kwh': number(row.get('generationEnergyKWh')),
            'self_consumption_energy_kwh': number(row.get('selfConsumptionEnergyKWh')),
            'loaded_at': now,
        })
    return output


def bronze_key(supply: dict, start_month: str, end_month: str) -> str:
    supply_key = hashlib.sha256(str(supply['cups']).encode()).hexdigest()[:16]
    return f'datadis/consumption/start={start_month.replace("/", "-")}/end={end_month.replace("/", "-")}/supply={supply_key}/data.json'


def save_bronze(payload: dict, supply: dict, start_month: str, end_month: str) -> dict:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
    checksum, bucket, key = hashlib.sha256(body).hexdigest(), os.getenv('DATADIS_BRONZE_BUCKET', 'tfm-bronze'), bronze_key(supply, start_month, end_month)
    client = boto3.client('s3', endpoint_url=os.getenv('DATADIS_S3_ENDPOINT', 'http://localhost:9000'),
                          aws_access_key_id=os.environ['MINIO_ROOT_USER'], aws_secret_access_key=os.environ['MINIO_ROOT_PASSWORD'],
                          region_name='us-east-1', config=Config(s3={'addressing_style': 'path'}))
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json',
                      Metadata={'source': 'datadis', 'sha256': checksum, 'record-count': str(len(payload['timeCurve'])),
                                'retrieved-at': datetime.now(timezone.utc).isoformat()})
    stored = client.get_object(Bucket=bucket, Key=key)['Body'].read()
    if hashlib.sha256(stored).hexdigest() != checksum:
        raise RuntimeError('Datadis Bronze: verificación de integridad fallida')
    return {'records': len(payload['timeCurve']), 'uri': f's3://{bucket}/{key}', 'sha256': checksum}


def chunks(rows, size=250):
    for index in range(0, len(rows), size):
        yield rows[index:index + size]


def load_silver(rows: list[dict]) -> dict:
    columns = ('cups distributor_code reading_date period consumption_kwh obtain_method surplus_energy_kwh '
               'generation_energy_kwh self_consumption_energy_kwh loaded_at').split()
    kinds = ('string string date string number string number number number timestamp').split()
    for group in chunks(rows):
        values = ', '.join('(' + ', '.join(_sql_value(row[column], kind) for column, kind in zip(columns, kinds)) + ')'
                           for row in group)
        updates = ', '.join(f'{column} = s.{column}' for column in columns if column not in ('cups', 'reading_date', 'period'))
        execute_trino(f'''MERGE INTO tfm.iot_silver.datadis_consumption_hourly t
USING (VALUES {values}) AS s ({', '.join(columns)})
ON t.cups = s.cups AND t.reading_date = s.reading_date AND t.period = s.period
WHEN MATCHED THEN UPDATE SET {updates}
WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES ({', '.join('s.' + column for column in columns)})''')
    return {'records': len(rows), 'table': 'tfm.iot_silver.datadis_consumption_hourly'}


def run(as_of: date | None = None) -> dict:
    as_of = as_of or datetime.now(timezone.utc).date()
    months = months_ending(as_of, int(os.getenv('DATADIS_LOOKBACK_MONTHS', '2')))
    token, all_rows, bronze = login(), [], []
    supply_list = supplies(token)
    request_delay = max(0, int(os.getenv('DATADIS_REQUEST_DELAY_SECONDS', '65')))
    for index, supply in enumerate(supply_list):
        rows = consumption(token, supply, months[0], months[-1])
        payload = {'source': 'datadis', 'startMonth': months[0], 'endMonth': months[-1], 'timeCurve': rows}
        bronze.append(save_bronze(payload, supply, months[0], months[-1]))
        all_rows.extend(transform(rows, supply))
        if index < len(supply_list) - 1 and request_delay:
            time.sleep(request_delay)
    if not all_rows:
        raise DatadisError('Datadis: no se recibieron lecturas')
    silver = load_silver(all_rows)
    LOG.info('Datadis cargado: supplies=%s records=%s', len(bronze), len(all_rows))
    return {'months': months, 'supplies': len(bronze), 'bronze': bronze, 'silver': silver}


def main():
    load_dotenv(Path(__file__).resolve().parents[2] / '.env', override=False)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--as-of', type=date.fromisoformat, help='Fecha de referencia YYYY-MM-DD')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    run(args.as_of)


if __name__ == '__main__':
    main()
