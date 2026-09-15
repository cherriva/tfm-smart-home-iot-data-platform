# Semana 3 — detección de anomalías

La capa de anomalías evalúa exclusivamente telemetría `synthetic` etiquetada. Así se
pueden medir las detecciones sin mezclar la evaluación con los datos reales del hogar.
El resultado se guarda en `tfm.iot_gold.anomaly_events`.

## Datos de prueba reproducibles

El generador sintético añade de forma determinista las etiquetas
`is_anomaly_expected`, `expected_anomaly_type` y
`anomaly_generation_version=week3-v1`. Las anomalías controladas incluyen temperatura
o humedad fuera de rango, cambios bruscos, calefacción ineficiente y luces encendidas
de madrugada.

Estas variables se configuran en `.env` antes de arrancar el stack:

```dotenv
SYNTHETIC_ANOMALIES_ENABLED=true
SYNTHETIC_ANOMALY_RATE=0.04
ANOMALY_MODEL_SEED=20260914
ANOMALY_CONTAMINATION=0.05
ANOMALY_TRAINING_DAYS=30
```

La semilla, la tasa de contaminación y el conjunto ordenado de `event_id` determinan
las versiones `model_version` y `dataset_version` que quedan registradas en Gold.

## Ejecución

El DAG de Airflow `anomaly_train_and_score` se ejecuta a las 02:15 y se puede lanzar
manualmente desde Airflow. Para ejecutarlo desde el contenedor de Airflow:

```bash
docker compose exec airflow python /opt/airflow/jobs/anomaly_pipeline.py
```

Se requieren al menos 20 eventos sintéticos numéricos recientes. El job separa
temporalmente el 70 % inicial para entrenamiento y usa el resto para evaluación. No
modifica Bronze ni Silver; antes de insertar elimina únicamente la ventana sintética
que vuelve a calcular en `iot_gold.anomaly_events`.

## Validación

Tras una ejecución correcta, el log devuelve `precision`, `recall`, `f1` y la matriz
de confusión. Esta consulta permite comprobar el resultado persistido:

```bash
docker compose exec trino trino --server http://localhost:8080 --user tfm \
  --execute "SELECT anomaly_type, severity, count(*) AS total
             FROM tfm.iot_gold.anomaly_events
             WHERE source = 'synthetic' AND combined_detected
             GROUP BY 1, 2 ORDER BY total DESC"
```

Cada fila contiene la regla o explicación estadística, el `anomaly_score`, el umbral,
el método que detectó el evento y la etiqueta esperada. Esto permite contrastar reglas,
Isolation Forest y el enfoque combinado de forma auditable.
