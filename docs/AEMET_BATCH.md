# Ingesta batch AEMET y Airflow

Canalización batch de semana 2: climatología diaria de AEMET a Bronze, Silver Iceberg y Gold ambiental diaria. AEMET es la fuente de los datos y debe citarse al reutilizarlos. No incluye todavía meteorología horaria.

## Arranque

Configurar `.env` a partir de `.env.example`, incluyendo AEMET_API_KEY. La clave no se incluye en imágenes ni en el repositorio. Ejecutar:

```bash
docker compose up -d --build
```

Airflow está en http://localhost:8082, usuario `admin`. Para consultar su contraseña local generada:

```bash
docker compose exec airflow cat /opt/airflow/state/simple_auth_passwords.json
```

Airflow 3.1.7 se ejecuta en modo `standalone`, con LocalExecutor y PostgreSQL 16.6. Este modo agrupa sus procesos para simplificar el despliegue local del TFM; no es una topología de producción. La base de metadatos, contraseña de acceso y logs persisten en volúmenes. Un servicio de inicialización prepara los permisos del volumen. No requiere montar el socket Docker.

## Programación

DAG: `aemet_daily_bronze` (el nombre se conserva por compatibilidad). Activo al crearse, todos los días a las 10:00 Europe/Madrid, sin recuperar automáticamente todo el histórico (`catchup=False`). Máximo una ejecución simultánea, tres reintentos separados 15 minutos y timeout de tarea de 10 minutos.

La fecha objetivo se calcula desde el final del intervalo de la ejecución menos `AEMET_LAG_DAYS` (4 por defecto). Es estable al reintentar. Los cuatro días son un margen operativo configurable, no una garantía de publicación de AEMET. Si los datos no están disponibles, la tarea falla y reintenta: no se marca éxito con un JSON vacío. Si agota los reintentos, se puede relanzar la fecha desde la interfaz o CLI.

Ejecución de una fecha explícita mediante el scheduler:

```bash
docker compose exec airflow airflow dags trigger aemet_daily_bronze --conf '{"date":"2026-09-09"}'
```

Ejecutar únicamente el extractor, sin crear un DAG run:

```bash
docker compose exec airflow python /opt/airflow/jobs/extract_aemet.py --date 2026-09-09
```

## Almacenamiento

Ruta Bronze: `s3://tfm-bronze/aemet/daily/year=YYYY/month=MM/day=DD/data.json`.

Se conserva el cuerpo original de la respuesta de datos, incluso su codificación. No se guarda la clave ni la URL temporal. Los metadatos S3 incluyen fecha objetivo, fuente, instante de descarga, número de registros y SHA-256. Se relee el objeto para comprobar su integridad.

El JSON crudo Bronze no se modifica. En la misma ejecución se selecciona la estación configurada (`AEMET_STATION_ID`, `9263D` por defecto), se conserva un JSONL de staging en `s3://tfm-silver/aemet/daily/.../data.jsonl` y se hace un `MERGE` idempotente en `tfm.iot_silver.aemet_daily`. dbt construye la vista canónica `tfm.gold.environment_daily`, que une el resumen interior real con AEMET por día y habitación.

Repetir una fecha escribe la misma clave de objeto y actualiza la fila Iceberg de estación/día mediante `MERGE`; no añade duplicados. Una corrección publicada por AEMET sustituye el contenido anterior y la fila derivada. Un fallo de API o validación no sobrescribe el objeto previo; si falla la carga posterior a Iceberg, Bronze queda conservado y el DAG falla para que el reintento complete la convergencia.

Validaciones: JSON no vacío, lista de registros, fecha solicitada en cada fila y estación presente sin repetición. Silver convierte decimales con coma y representa precipitación especial y valores ausentes como nulos. La lectura respeta UTF-8 o Latin-1, sin corromper nombres de estación.

## Entorno virtual local

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r airflow/requirements.txt
.venv/bin/python airflow/jobs/extract_aemet.py --date 2026-09-09
```

El extractor carga automáticamente el `.env` de la raíz, también al ejecutarlo desde el venv. No modifica el script `activate` ni incrusta claves en él. En local usa MinIO en `http://localhost:9000`; Docker configura `http://minio:9000`. Se puede cambiar con `AEMET_S3_ENDPOINT`.

Pruebas sin peticiones reales:

```bash
.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
docker compose config --quiet
docker compose exec airflow airflow dags list-import-errors -o json
```

## Referencias

- API oficial: https://opendata.aemet.es/dist/
- Airflow standalone: https://airflow.apache.org/docs/apache-airflow/3.1.7/start.html

## Verificación end-to-end

El DAG encadena descarga Bronze, `MERGE` en Silver Iceberg, `dbt build` y una consulta Trino que exige al menos una fila AEMET y una fila Gold. Para una fecha ya descargada puede repetirse la ejecución manual: el resultado debe mantener una fila por estación/día en Silver.
