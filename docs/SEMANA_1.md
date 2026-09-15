# Semana 1: Lakehouse Iceberg y calidad

## Alcance implementado

El flujo operativo es MQTT/TLS → Redpanda → Spark Structured Streaming → Bronze
Iceberg → Silver/cuarentena → Gold Iceberg → Trino → Grafana. Un único job
`spark-lakehouse` procesa cada microbatch en ese orden, usando MERGE idempotentes.
Los jobs Parquet y su sincronizador permanecen disponibles en el perfil `legacy`.

| Entregable | Ubicación |
|---|---|
| Baseline congelado | `docs/evidence/baseline.json` |
| Contrato y reglas | `docs/CONTRATO_EVENTOS_V1.md` |
| Normalización compartida | `spark-lakehouse/event_contract.py` |
| Catálogo, tablas y MERGE | `spark-lakehouse/lakehouse.py` |
| Streaming e importación | `spark-lakehouse/lakehouse_stream.py` |
| Vistas compatibles y calidad | `trino/iceberg-views.sql` |
| Pruebas unitarias | `tests/test_event_contract.py` |
| Integración real con Iceberg | `spark-lakehouse/verify_iceberg.py` |
| Smoke MQTT/Kafka → Grafana | `scripts/smoke_test.py` |

## Catálogos y versiones

| Motor/componente | Versión / función |
|---|---|
| Spark / Scala | 3.5.3 / 2.12 |
| Iceberg runtime | `iceberg-spark-runtime-3.5_2.12:1.7.2` |
| Hadoop AWS / AWS SDK v1 | 3.3.4 / 1.12.262 |
| Trino | 483, conector `iceberg` en catálogo `tfm` |
| Hive Metastore Iceberg | 3.1.3, volumen independiente `iceberg_metastore_data` |
| Hive Metastore anterior | 4.2.1, volumen original intacto, catálogo `tfm_legacy` |
| Formato de tabla / fichero | Iceberg v2 / Parquet |
| Bucket nuevo | `tfm-lakehouse` |
| Checkpoint nuevo | `s3a://tfm-checkpoints/iceberg_v1` |

La prueba inicial contra Hive 4.2.1 falló con `Invalid method name: get_table`:
Spark incluye un cliente Hive antiguo que no puede usar esa API eliminada en Hive 4.
Se aisló el catálogo Iceberg en un Hive 3.1.3 sin degradar ni reutilizar la base Derby
de Hive 4.2.1. Ambos metastore son locales y usan volúmenes distintos. El nuevo no
publica un puerto al host. Es una decisión de compatibilidad y migración reversible;
para un despliegue distribuido posterior, conviene sustituir Derby por una base
de metadatos externa. Las dependencias Spark se descargan al construir la imagen,
por lo que un reinicio no requiere acceso a Maven.

Documentación primaria consultada:

- [Catálogos y extensiones Spark de Iceberg](https://iceberg.apache.org/docs/latest/spark-configuration/).
- [MERGE y escrituras Spark de Iceberg](https://iceberg.apache.org/docs/latest/spark-writes/).
- [Conector Iceberg de Trino](https://trino.io/docs/current/connector/iceberg.html).
- [Incidencia Apache sobre get_table e Hive 4](https://github.com/apache/iceberg/issues/12878).

## Tablas

| Tabla física de Trino | Contenido |
|---|---|
| `tfm.iot_bronze.matter_events` | Una fila por entrega Kafka, crudo y evaluación de calidad |
| `tfm.iot_silver.matter_events` | Eventos aceptados, únicos por event_id |
| `tfm.iot_quality.quarantine_events` | Entregas inválidas y todas sus causas |
| `tfm.iot_gold.entity_5m` | Agregados de 5 min por source, entidad, dominio y unidad |
| `tfm.iot_quality.sensor_inventory` | Sensores conocidos y ventana de actividad |

Spark usa los mismos nombres con catálogo `lake`. Las vistas `tfm.bronze.matter_events`,
`tfm.silver.matter_events` y `tfm.gold.entity_5m` mantienen los nombres usados por
Grafana. En `tfm_legacy.bronze/silver/gold` siguen consultándose las tablas Parquet.
Las vistas `tfm.iot_quality.events`, `duplicate_events` y `sensor_activity` completan
la auditoría. El dashboard 04 incluye tres paneles adicionales y mide calidad por
recepción Kafka para incluir registros sin fecha de evento válida.

La presentación de Grafana combina el histórico real y sintético sin mostrar su
procedencia. Los identificadores y nombres se normalizan únicamente en las consultas de
los paneles; las tablas Iceberg conservan `source`, `original_entity_id`, `dataset_id` y
el resto de la trazabilidad. La separación privada puede consultarse con
`.venv/bin/python scripts/source_audit.py`.

## Arranque normal

Desde el directorio del proyecto, con `.env` configurado:

```bash
docker compose up -d --build
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python scripts/smoke_test.py --restart
```

`minio-init` crea buckets; `lakehouse-init` crea tablas e inventario;
`trino-iceberg-init` provisiona vistas. El streaming espera a la inicialización de
tablas. Los antiguos jobs Parquet no arrancan salvo que se pida su perfil. El
ingestor necesita el broker externo MQTT y sus credenciales; la generación sintética
entra directamente por Kafka. Para verificar sin conectividad doméstica:

```bash
.venv/bin/python scripts/smoke_test.py --transport kafka --restart
```

El smoke publica exclusivamente telemetría de prueba con `source=synthetic`,
`dataset_id=week1_<id>` y una entidad dedicada. No publica comandos de dispositivos.
La prueba MQTT recorre el broker TLS y el ingestor reales. La alternativa Kafka no
demuestra el tramo MQTT y lo deja reflejado en la evidencia. Ambas conservan sus
pocas filas de prueba para auditoría. `--restart` reinicia el servicio Spark y
vuelve a enviar el mismo evento, esperando que Silver y Gold mantengan el conteo.

Las consultas de Grafana se ejecutan a través de su API de datasource (incluida la
expansión de macros), usando las credenciales locales de Compose solo en memoria.
Si se cambió la contraseña de Grafana en la UI, actualizar la configuración local
para que el script pueda autenticarse. El script falla ante un panel con error.

## Importación desde el baseline Parquet

La migración no reutiliza checkpoints ni modifica ficheros Parquet. Importa Bronze
completa antes de releer los offsets disponibles de Kafka; el solapamiento se
resuelve por `observation_id`. Así también se conserva el histórico que ya haya
caducado por retención Kafka. Silver y Gold se derivan de nuevo aplicando el contrato.

```bash
docker compose --profile legacy stop spark-bronze spark-silver-gold trino-sync
docker compose stop spark-lakehouse
docker compose up -d --wait minio iceberg-metastore
docker compose run --rm minio-init
docker compose run --rm --no-deps spark-lakehouse \
  /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 1536m \
  /opt/spark/jobs/lakehouse_stream.py --bootstrap-parquet
docker compose up -d spark-lakehouse trino-iceberg-init
```

Es seguro repetir la importación **con el escritor streaming detenido**. Nunca
ejecutar dos escritores de producción al mismo tiempo. Las tablas conservan la
primera clasificación Bronze confirmada; no usar la importación como mecanismo
implícito de cambio de reglas sobre el histórico.

## Pruebas de integración

```bash
docker compose run --rm --no-deps spark-lakehouse \
  /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 1536m \
  /opt/spark/jobs/verify_iceberg.py
```

Utiliza un namespace aleatorio exclusivo de prueba, comprueba MERGE con duplicados,
históricos, rangos, fechas futuras, JSON inválido, reintentos y recuperación tras
un fallo entre Silver y Gold. Elimina solo sus propias tablas al terminar con éxito.
Si falla, deja ese namespace para diagnóstico. Requiere MinIO y el metastore en
marcha. Se recomienda detener temporalmente Spark de producción en equipos con
memoria limitada al ejecutar esta prueba para no solapar dos JVM Spark.

Para comprobar el formato físico, usar las tablas `iot_*`, no las vistas compatibles:

```sql
SHOW CREATE TABLE tfm.iot_silver.matter_events;
SELECT * FROM tfm.iot_silver."matter_events$snapshots";
SELECT count(*) - count(DISTINCT event_id) AS duplicates
FROM tfm.iot_silver.matter_events;
SELECT quality_errors, count(*) FROM tfm.iot_quality.quarantine_events GROUP BY 1;
SELECT activity_status, count(*) FROM tfm.iot_quality.sensor_activity GROUP BY 1;
```

## Volver al baseline local

La configuración anterior y sus dashboards se archivaron fuera del proyecto en
`../TFM_ARCHIVO_2026-09-13/rollback-local/baseline/` antes de editar. Es una copia
local de rollback, no un mecanismo de distribución. Para inspección suele bastar
consultar `tfm_legacy` sin revertir.
Si se necesita volver al flujo anterior en esta instalación:

1. Detener `spark-lakehouse` y conservar sus volúmenes y checkpoint.
2. Restaurar los archivos guardados en `../TFM_ARCHIVO_2026-09-13/rollback-local/baseline/` (Compose, Trino, Grafana,
   `spark-bronze`, README) sobre sus ubicaciones originales.
3. Reiniciar Trino y Grafana, y levantar los dos jobs Spark Parquet y `trino-sync`.
4. No borrar volúmenes ni ejecutar `docker compose down -v`.

La copia local del baseline no contiene `.env`. Las credenciales permanecen en su
ubicación original. Los artefactos de evidencia incluyen ejemplos sin entity_id,
atributos domésticos ni secretos. La revisión general para publicar GitHub, la
rotación de credenciales y CI siguen en la semana 4.

## Límites de esta semana

- La unicidad es responsabilidad del MERGE y del único escritor, no una restricción
  UNIQUE de Iceberg. Los commits son atómicos por tabla y convergen entre capas al
  reintentar; puede haber desfase temporal mientras termina un microbatch.
- No hay compactación programada ni expiración de snapshots todavía; corresponde
  a la orquestación de la semana 2. Aumentar el intervalo de microbatch permite
  limitar la proliferación de ficheros mientras tanto.
- Un evento retrasado puede ser correcto y se conserva con advertencia; un sensor
  sin cambios puede estar sano aunque figure sin actividad. Son métricas de datos.
- El banco de pruebas y el despliegue son locales, sin alta disponibilidad.
- Airflow, dbt, contexto, ML, benchmark formal y memoria quedan para semanas 2–4.
