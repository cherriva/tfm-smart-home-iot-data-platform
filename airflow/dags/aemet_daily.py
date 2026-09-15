"""Daily ingestion; explicit date is supported through DAG run configuration."""
import os
from datetime import date, timedelta

import pendulum
from airflow.sdk import dag, task, get_current_context


@dag(dag_id='aemet_daily_bronze', schedule='0 10 * * *',
     start_date=pendulum.datetime(2026, 9, 1, tz='Europe/Madrid'),
     catchup=False, max_active_runs=1, is_paused_upon_creation=False,
     default_args={'retries': 3, 'retry_delay': timedelta(minutes=15)},
     tags=['aemet', 'batch', 'bronze', 'silver', 'gold'],
     description='Climatología diaria AEMET: Bronze original, Silver Iceberg y Gold ambiental')
def aemet_daily():
    @task(execution_timeout=timedelta(minutes=10))
    def ingest():
        from extract_aemet import run
        context = get_current_context()
        conf = context['dag_run'].conf or {}
        if conf.get('date'):
            target = date.fromisoformat(conf['date'])
        else:
            # Stable across retries, based on the scheduled interval boundary.
            anchor = context.get('data_interval_end') or context['dag_run'].run_after
            target = anchor.in_timezone('Europe/Madrid').date() - timedelta(days=int(os.getenv('AEMET_LAG_DAYS', '4')))
        return run(target)
    @task(execution_timeout=timedelta(minutes=5))
    def build_dbt():
        import subprocess
        result = subprocess.run(
            ['dbt', 'build', '--project-dir', '/opt/airflow/dbt', '--profiles-dir', '/opt/airflow/dbt'],
            cwd='/opt/airflow/dbt', text=True, capture_output=True, timeout=240,
        )
        print(result.stdout, flush=True)
        if result.returncode:
            print(result.stderr, flush=True)
            raise RuntimeError(f'dbt build terminó con código {result.returncode}')
        return 'dbt build OK'

    @task(execution_timeout=timedelta(minutes=5))
    def validate_gold():
        import requests
        response = requests.post(
            'http://trino:8080/v1/statement',
            headers={'X-Trino-User': 'airflow', 'Content-Type': 'text/plain'},
            data="SELECT (SELECT count(*) FROM tfm.iot_silver.aemet_daily) AS aemet_rows, "
                 "count(*) AS gold_rows, count(DISTINCT room_name) AS rooms FROM tfm.gold.environment_daily",
            timeout=120,
        )
        response.raise_for_status()
        payload = response.json()
        while payload.get('nextUri'):
            response = requests.get(payload['nextUri'], headers={'X-Trino-User': 'airflow'}, timeout=120)
            response.raise_for_status()
            payload = response.json()
        if payload.get('error'):
            raise RuntimeError('Validación Gold Trino fallida: ' + payload['error'].get('message', 'error'))
        data = payload.get('data') or []
        result = {'aemet_rows': int(data[0][0]) if data else 0,
                  'gold_rows': int(data[0][1]) if data else 0,
                  'rooms': int(data[0][2]) if data else 0}
        if result['aemet_rows'] == 0 or result['gold_rows'] == 0:
            raise RuntimeError('Silver AEMET o Gold ambiental vacía')
        print('Gold ambiental validada:', result, flush=True)
        return result

    ingest() >> build_dbt() >> validate_gold()


aemet_daily()
