FROM apache/airflow:3.1.7-python3.12
COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.1.7" psycopg2-binary==2.9.11 boto3==1.42.30 requests==2.32.5 python-dotenv==1.2.1 scikit-learn==1.8.0 numpy==2.4.2 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.7/constraints-3.12.txt"
RUN pip install --no-cache-dir dbt-core==1.10.15 dbt-trino==1.9.3
COPY --chown=airflow:root airflow/dags /opt/airflow/dags
COPY --chown=airflow:root airflow/jobs /opt/airflow/jobs
COPY --chown=airflow:root dbt /opt/airflow/dbt
ENV PYTHONPATH=/opt/airflow/jobs
