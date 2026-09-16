"""Daily, replay-safe Datadis consumption ingestion."""
from datetime import date, timedelta
import subprocess

import pendulum
from airflow.sdk import dag, get_current_context, task


@dag(dag_id='datadis_daily_consumption', schedule='30 11 * * *',
     start_date=pendulum.datetime(2026, 9, 1, tz='Europe/Madrid'), catchup=False,
     max_active_runs=1, is_paused_upon_creation=False,
     default_args={'retries': 3, 'retry_delay': timedelta(minutes=15)},
     tags=['datadis', 'energy', 'batch', 'bronze', 'silver'],
     description='Curva horaria Datadis: Bronze original y Silver Iceberg idempotente')
def datadis_daily():
    @task(execution_timeout=timedelta(minutes=15))
    def ingest():
        from extract_datadis import run
        context = get_current_context()
        configured = (context['dag_run'].conf or {}).get('as_of')
        if configured:
            target = date.fromisoformat(configured)
        else:
            anchor = context.get('data_interval_end') or context['dag_run'].run_after
            target = pendulum.instance(anchor).in_timezone('Europe/Madrid').date()
        return run(target)
    @task(execution_timeout=timedelta(minutes=5))
    def build_gold():
        result = subprocess.run(['dbt', 'build', '--select', '+gold_grid_energy_daily',
                                 '--project-dir', '/opt/airflow/dbt', '--profiles-dir', '/opt/airflow/dbt'],
                                cwd='/opt/airflow/dbt', text=True, capture_output=True, timeout=240)
        print(result.stdout, flush=True)
        if result.returncode:
            print(result.stderr, flush=True)
            raise RuntimeError(f'dbt Datadis terminó con código {result.returncode}')
    ingest() >> build_gold()


datadis_daily()
