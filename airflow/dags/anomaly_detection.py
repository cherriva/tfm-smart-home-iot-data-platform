"""Daily reproducible training and scoring of the Week 3 anomaly layer."""
from datetime import timedelta
import pendulum
from airflow.sdk import dag, task


@dag(dag_id='anomaly_train_and_score', schedule='15 2 * * *',
     start_date=pendulum.datetime(2026, 9, 14, tz='Europe/Madrid'), catchup=False,
     max_active_runs=1, is_paused_upon_creation=False,
     default_args={'retries': 2, 'retry_delay': timedelta(minutes=10)},
     tags=['ml', 'anomaly', 'gold'], description='Reglas + Isolation Forest reproducible sobre Silver')
def anomaly_train_and_score():
    @task(execution_timeout=timedelta(minutes=15))
    def train_score():
        from anomaly_pipeline import run
        return run()
    train_score()


anomaly_train_and_score()
