"""AEMET daily climatology -> immutable Bronze object and Iceberg Silver."""
import argparse
import hashlib
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import boto3
import requests
from botocore.config import Config
from dotenv import load_dotenv

BASE_URL = 'https://opendata.aemet.es/opendata/api'
LOG = logging.getLogger(__name__)


class AemetError(RuntimeError):
    pass


def get_response(url, headers=None, timeout=60):
    # Never include requests exceptions/temporary URLs in logs: they may contain tokens.
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException:
        raise AemetError('AEMET: fallo de conexión o timeout') from None
    if response.status_code != 200:
        raise AemetError(f'AEMET HTTP {response.status_code}')
    return response


def extract_daily_weather(api_key: str, target_date: date) -> bytes:
    start = f'{target_date.isoformat()}T00:00:00UTC'
    end = f'{target_date.isoformat()}T23:59:59UTC'
    url = f'{BASE_URL}/valores/climatologicos/diarios/datos/fechaini/{start}/fechafin/{end}/todasestaciones'
    response = get_response(url, headers={'api_key': api_key}, timeout=30)
    try:
        metadata = response.json()
    except ValueError:
        raise AemetError('AEMET: respuesta de metadatos no válida') from None
    if not isinstance(metadata, dict) or metadata.get('estado') != 200:
        status = metadata.get('estado') if isinstance(metadata, dict) else 'inválido'
        raise AemetError(f'AEMET estado {status}: datos no disponibles o solicitud rechazada')
    data_url = metadata.get('datos', '')
    parsed = urlparse(data_url)
    if parsed.scheme != 'https' or parsed.hostname != 'opendata.aemet.es':
        raise AemetError('AEMET: URL de descarga inesperada')
    response = get_response(data_url)
    payload = response.content
    validate_payload(payload, target_date)
    return payload


def validate_payload(payload: bytes, target_date: date) -> list[dict]:
    try:
        # AEMET can serve JSON encoded as Latin-1.
        try:
            text = payload.decode('utf-8-sig')
        except UnicodeDecodeError:
            text = payload.decode('latin-1')
        rows = json.loads(text)
    except (ValueError, UnicodeError):
        raise AemetError('AEMET: JSON de datos no válido') from None
    if not isinstance(rows, list) or not rows:
        raise AemetError('AEMET: dataset vacío o formato inesperado')
    seen = set()
    for row in rows:
        if not isinstance(row, dict) or row.get('fecha') != target_date.isoformat() or not row.get('indicativo'):
            raise AemetError('AEMET: fecha o identificador de estación inválidos')
        if row['indicativo'] in seen:
            raise AemetError('AEMET: estación duplicada en el día')
        seen.add(row['indicativo'])
    return rows


def decode_payload(payload: bytes) -> list[dict]:
    """Decode AEMET's UTF-8 or Latin-1 JSON without changing station names."""
    try:
        text = payload.decode('utf-8-sig')
    except UnicodeDecodeError:
        text = payload.decode('latin-1')
    rows = json.loads(text)
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        raise AemetError('AEMET: dataset vacío o formato inesperado')
    return rows


def bronze_key(target_date: date) -> str:
    return f'aemet/daily/year={target_date.year}/month={target_date.month:02d}/day={target_date.day:02d}/data.json'


def save_bronze(payload: bytes, target_date: date) -> dict:
    rows = validate_payload(payload, target_date)
    endpoint = os.getenv('AEMET_S3_ENDPOINT', 'http://localhost:9000')
    client = boto3.client('s3', endpoint_url=endpoint,
                          aws_access_key_id=os.environ['MINIO_ROOT_USER'],
                          aws_secret_access_key=os.environ['MINIO_ROOT_PASSWORD'],
                          region_name='us-east-1', config=Config(s3={'addressing_style': 'path'}))
    bucket = os.getenv('AEMET_BRONZE_BUCKET', 'tfm-bronze')
    key = bronze_key(target_date)
    checksum = hashlib.sha256(payload).hexdigest()
    client.put_object(Bucket=bucket, Key=key, Body=payload, ContentType='application/json',
                      Metadata={'source': 'aemet', 'target-date': target_date.isoformat(),
                                'sha256': checksum, 'record-count': str(len(rows)),
                                'retrieved-at': datetime.now(timezone.utc).isoformat()})
    stored = client.get_object(Bucket=bucket, Key=key)
    if hashlib.sha256(stored['Body'].read()).hexdigest() != checksum:
        raise RuntimeError('Bronze: verificación de integridad fallida')
    result = {'date': target_date.isoformat(), 'records': len(rows), 'uri': f's3://{bucket}/{key}', 'sha256': checksum}
    LOG.info('Ingesta verificada: %s', result)
    return result


def _number(value):
    if value in (None, '', 'Ip', 'I', 'Acum', 'Trace'):
        return None
    try:
        return float(str(value).replace(',', '.'))
    except (TypeError, ValueError):
        return None


def silver_key(target_date: date) -> str:
    return f'aemet/daily/year={target_date.year}/month={target_date.month:02d}/day={target_date.day:02d}/data.jsonl'


def transform_silver(payload: bytes, station_id: str = '9263D') -> list[dict]:
    decoded = decode_payload(payload)
    rows = validate_payload(payload, date.fromisoformat(decoded[0]['fecha']))
    selected = [r for r in rows if r.get('indicativo') == station_id]
    if not selected:
        raise AemetError(f'AEMET: estación {station_id} no encontrada')
    output = []
    for row in selected:
        output.append({
            'source': 'aemet', 'station_id': row['indicativo'],
            'station_name': row.get('nombre'), 'province': row.get('provincia'),
            'altitude_m': _number(row.get('altitud')), 'observation_date': row['fecha'],
            'mean_temperature_c': _number(row.get('tmed')), 'min_temperature_c': _number(row.get('tmin')),
            'max_temperature_c': _number(row.get('tmax')), 'precipitation_mm': _number(row.get('prec')),
            'mean_humidity_pct': _number(row.get('hrMedia')), 'min_humidity_pct': _number(row.get('hrMin')),
            'max_humidity_pct': _number(row.get('hrMax')), 'mean_wind_speed_kmh': _number(row.get('velmedia')),
            'max_gust_kmh': _number(row.get('racha')), 'sunshine_hours': _number(row.get('sol')),
            'loaded_at': datetime.now(timezone.utc).isoformat(),
        })
    return output


def save_silver(payload: bytes, target_date: date, station_id: str = '9263D') -> dict:
    records = transform_silver(payload, station_id)
    endpoint = os.getenv('AEMET_S3_ENDPOINT', 'http://localhost:9000')
    client = boto3.client('s3', endpoint_url=endpoint, aws_access_key_id=os.environ['MINIO_ROOT_USER'],
                          aws_secret_access_key=os.environ['MINIO_ROOT_PASSWORD'], region_name='us-east-1',
                          config=Config(s3={'addressing_style': 'path'}))
    bucket = os.getenv('AEMET_SILVER_BUCKET', 'tfm-silver')
    key = silver_key(target_date)
    body = ('\n'.join(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in records) + '\n').encode()
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/x-ndjson',
                      Metadata={'source': 'aemet', 'station-id': station_id, 'target-date': target_date.isoformat(),
                                'record-count': str(len(records))})
    return {'date': target_date.isoformat(), 'station_id': station_id, 'records': len(records), 'uri': f's3://{bucket}/{key}'}


def _sql_value(value, kind='string'):
    if value is None:
        return 'NULL'
    if kind == 'number':
        return repr(float(value))
    if kind == 'date':
        return f"DATE '{value}'"
    if kind == 'timestamp':
        return "from_iso8601_timestamp('" + str(value).replace("'", "''") + "')"
    return "'" + str(value).replace("'", "''") + "'"


def execute_trino(statement: str) -> None:
    """Run one statement and surface Trino errors after following the result pages."""
    endpoint = os.getenv('TRINO_ENDPOINT', 'http://trino:8080/v1/statement')
    headers = {'X-Trino-User': 'airflow', 'Content-Type': 'text/plain'}
    try:
        response = requests.post(endpoint, headers=headers, data=statement, timeout=120)
        response.raise_for_status()
        payload = response.json()
        while payload.get('nextUri'):
            response = requests.get(payload['nextUri'], headers={'X-Trino-User': 'airflow'}, timeout=120)
            response.raise_for_status()
            payload = response.json()
    except requests.RequestException:
        raise AemetError('Trino: fallo de conexión o timeout al cargar Silver') from None
    if payload.get('error'):
        raise AemetError('Trino: carga Silver fallida: ' + payload['error'].get('message', 'error'))


def load_silver_iceberg(records: list[dict]) -> dict:
    """Idempotently upsert the selected station/day into the governed Iceberg table."""
    columns = ('source station_id station_name province altitude_m observation_date mean_temperature_c '
               'min_temperature_c max_temperature_c precipitation_mm mean_humidity_pct min_humidity_pct '
               'max_humidity_pct mean_wind_speed_kmh max_gust_kmh sunshine_hours loaded_at').split()
    kinds = ('string string string string number date number number number number number number number '
             'number number number timestamp').split()
    values = []
    for record in records:
        values.append('(' + ', '.join(_sql_value(record[column], kind) for column, kind in zip(columns, kinds)) + ')')
    updates = ', '.join(f'{column} = s.{column}' for column in columns if column not in ('station_id', 'observation_date'))
    statement = f'''MERGE INTO tfm.iot_silver.aemet_daily t
USING (VALUES {', '.join(values)}) AS s ({', '.join(columns)})
ON t.station_id = s.station_id AND t.observation_date = s.observation_date
WHEN MATCHED THEN UPDATE SET {updates}
WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES ({', '.join('s.' + column for column in columns)})'''
    execute_trino(statement)
    return {'records': len(records), 'table': 'tfm.iot_silver.aemet_daily'}


def run(target_date: date) -> dict:
    key = os.getenv('AEMET_API_KEY')
    if not key:
        raise AemetError('Falta AEMET_API_KEY en el entorno local')
    if target_date >= datetime.now(timezone.utc).date():
        raise ValueError('La fecha debe corresponder a un día finalizado')
    payload = extract_daily_weather(key, target_date)
    bronze = save_bronze(payload, target_date)
    silver = save_silver(payload, target_date, os.getenv('AEMET_STATION_ID', '9263D'))
    records = transform_silver(payload, os.getenv('AEMET_STATION_ID', '9263D'))
    iceberg = load_silver_iceberg(records)
    return {'bronze': bronze, 'silver': silver, 'iceberg': iceberg}


def main():
    load_dotenv(Path(__file__).resolve().parents[2] / '.env', override=False)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date', type=date.fromisoformat, help='YYYY-MM-DD; defecto: hoy menos AEMET_LAG_DAYS (4)')
    args = parser.parse_args()
    target = args.date or (datetime.now(timezone.utc).date() - timedelta(days=int(os.getenv('AEMET_LAG_DAYS', '4'))))
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    run(target)


if __name__ == '__main__':
    main()
