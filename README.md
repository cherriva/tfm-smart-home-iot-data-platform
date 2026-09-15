# TFM Smart Home IoT

La documentación funcional y operativa está en [docs](docs/).

Lakehouse de la semana 1: Apache Iceberg, Silver idempotente y calidad auditable.
El procedimiento, las decisiones y las pruebas están en [docs/SEMANA_1.md](docs/SEMANA_1.md),
y el modelo de datos en [docs/CONTRATO_EVENTOS_V1.md](docs/CONTRATO_EVENTOS_V1.md).

Canalización ejecutable:

```text
Matter ---------> Home Assistant -> MQTT/TLS -> mqtt-ingestor -> Redpanda -> Spark -> MinIO Iceberg Bronze/Silver/Gold -> Trino -> Grafana
HomeKit --------/                                      ^
                                             synthetic-generator
```

## Arranque

1. Crear un entorno local para las utilidades y pruebas:

   ```bash
   python3 -m venv .venv
   .venv/bin/python -m pip install -r airflow/requirements.txt
   ```

2. Crear el fichero local de variables:

   ```bash
   cp .env.example .env
   ```

3. Editar `.env` y completar `MQTT_PASSWORD`. Las credenciales de MinIO y Grafana
   tienen valores locales de desarrollo que conviene cambiar si se expone el stack.

4. Arrancar el stack de ingesta:

   ```bash
   docker compose up -d --build
   ```

5. Revisar el estado:

   ```bash
   docker compose ps
   docker compose logs -f mqtt-ingestor
   ```

## Comprobación

- Redpanda Console: http://localhost:8080
- Grafana: http://localhost:3000
- MinIO Console: http://localhost:9001
- Filtro MQTT de entrada: `tfm/+/events/#`. Incluye `tfm/matter/events/#`,
  `tfm/sonoff/events/#` y cualquier otro origen compatible con el contrato.
  `tfm/homekit/events/#`.
- Tópico de destino en Redpanda: `tfm.matter.events` (nombre histórico; contiene eventos Matter, HomeKit y Sonoff).
- El broker Kafka se publica localmente en `localhost:19092`.
- El ingestor usa internamente `redpanda:9092`; no hace falta exponer Kafka a Internet.
- `synthetic-generator` crea un histórico de demostración de 24 horas y continúa generando
  datos cada 5 minutos para todas las entidades activas del inventario.
- `spark-lakehouse` escribe tablas Iceberg v2 en `s3a://tfm-lakehouse` y usa el checkpoint `s3a://tfm-checkpoints/iceberg_v1`.
- El baseline Parquet sigue disponible en el catálogo `tfm_legacy`; los jobs antiguos están detenidos en el perfil `legacy`.
- Trino expone las tablas `tfm.bronze.matter_events`, `tfm.silver.matter_events` y `tfm.gold.entity_5m` por SQL en http://localhost:8081.

Para consumir una muestra desde el contenedor de Redpanda:

```bash
docker compose exec redpanda rpk topic consume tfm.matter.events --brokers redpanda:9092 -n 1
```

El ingestor conserva el JSON de Home Assistant y añade `mqtt_topic` y `bridge_timestamp`.

## Sensores ambientales HomeKit

La automatización `TFM - HomeKit - publicar sensores ambientales en MQTT` publica por
MQTT/TLS los cambios de seis entidades de temperatura y humedad expuestas en Home
Assistant. Utiliza el espacio de tópicos `tfm/homekit/events/sensor/<entity_id>` y marca
cada evento con `source=homekit` y `attributes.data_origin=homekit`.

Las seis entidades HomeKit integradas son:

- `sensor.temperatura_salon`
- `sensor.temperatura_dormitorio_padres`
- `sensor.temperatura_dormitorio_borja`
- `sensor.humedad_salon`
- `sensor.humedad_dormitorio_borja`
- `sensor.humedad_dormitorio_padres`

Recorren la misma canalización que Matter: Redpanda, Bronze, Silver, Gold, Trino y
Grafana. El dashboard ambiental los incorpora automáticamente y el dashboard de estado
incluye una sección específica con sus últimos valores.

## Datos sintéticos

El generador sintético está incluido en `docker compose` y se activa por defecto. Publica
directamente en Redpanda, por lo que no modifica Home Assistant ni el broker MQTT externo.
Los datos recorren la canalización completa desde Bronze hasta Grafana.

Cada entidad sintética conserva su dominio y utiliza el prefijo `synthetic_`. Por ejemplo:

```text
sensor.temperatura_bano_3 -> sensor.synthetic_temperatura_bano_3
light.salon_lampara_encendido_canal_1 -> light.synthetic_salon_lampara_encendido_canal_1
```

El `entity_id` con prefijo es el identificador estable de cada entidad sintética y
`original_entity_id` mantiene la correspondencia con la entidad real. Cada evento incluye
además `source=synthetic` y, dentro de `attributes`, un `synthetic_id` UUID único,
`dataset_id`, `inventory_device_id`, `area_id`, `manufacturer` y `model`. Así pueden
distinguirse y auditarse en Bronze y Silver; en Gold continúan siendo reconocibles por el
campo `source` y el prefijo de la entidad.

Grafana presenta el histórico como un único conjunto de datos del hogar: no muestra el
origen, no compara eventos reales y sintéticos y normaliza los nombres de las entidades.
La procedencia se conserva íntegra en Iceberg. Para verla de forma privada:

```bash
.venv/bin/python scripts/source_audit.py
```

El histórico inicial es idempotente: su progreso se conserva en el volumen
`synthetic_generator_state` y no se vuelve a insertar al reiniciar los contenedores. Para
desactivar la generación basta con establecer:

```dotenv
SYNTHETIC_ENABLED=false
```

Las variables `SYNTHETIC_BOOTSTRAP_HOURS`, `SYNTHETIC_BOOTSTRAP_STEP_SECONDS` y
`SYNTHETIC_INTERVAL_SECONDS` controlan el histórico y la frecuencia continua.

El job Spark conserva también `raw_value` y añade los metadatos Kafka, `bronze_timestamp`,
`event_date` y el identificador de observación. Las tablas de eventos se particionan por día de recepción Kafka.

Silver admite eventos históricos aunque lleguen después de eventos actuales. Silver deduplica por un `event_id`
determinista que excluye metadatos de transporte. Los inválidos quedan en cuarentena con causas
explícitas; los retrasados válidos permanecen en Silver con advertencia. Gold recalcula desde
Silver las ventanas de 5 minutos afectadas, incluidas las históricas.

Spark y Trino comparten el catálogo Iceberg sobre un Hive Metastore 3.1.3 persistente.
El metastore 4.2.1 original conserva el baseline Parquet. Iceberg gestiona sus particiones y
snapshots sin `trino-sync`. Las vistas compatibles conservan los nombres usados por Grafana.
Las tablas físicas son `tfm.iot_bronze.matter_events`, `tfm.iot_silver.matter_events`,
`tfm.iot_gold.entity_5m` y `tfm.iot_quality.quarantine_events`.

Grafana se provisiona automáticamente con el plugin de Trino, la fuente de datos `Trino`
y cinco dashboards: visión general, sensores ambientales, actividad de dispositivos,
calidad de la plataforma IoT y estado actual por entidad. El usuario inicial es `admin` y la contraseña se toma de
`GRAFANA_ADMIN_PASSWORD`.

Los dashboards están en la carpeta `TFM Smart Home` y se actualizan cada 30 segundos:

- `01 - Visión general del hogar`: Bronze, Silver, Gold, actividad y últimos eventos.
- `02 - Sensores ambientales`: temperatura, humedad, estadísticas y último valor conocido.
- `03 - Actividad de dispositivos`: estados, luces, interruptores, botones, puertas y cerraduras.
- `04 - Calidad de la plataforma IoT`: validez, latencia, cobertura temporal, cuarentena,
  duplicados y sensores sin actividad.
- `05 - Panel de control por dispositivo`: último KPI, estado, estancia y actualización de
  cada entidad activa, con vistas específicas de ambiente, accesos, luces e interruptores.

Para actualizar las vistas y dashboards en una instalación con las tablas Iceberg creadas:

```bash
docker compose run --rm trino-iceberg-init
docker compose restart grafana
```

Consultas rápidas en Trino:

```bash
docker compose exec trino trino --server http://localhost:8080 --user tfm \
  --execute "SELECT count(*) FROM tfm.silver.matter_events"

docker compose exec trino trino --server http://localhost:8080 --user tfm \
  --execute "SELECT domain, count(*) FROM tfm.gold.entity_5m GROUP BY domain ORDER BY domain"
```

## Verificación automática

```bash
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python scripts/smoke_test.py --restart
```

El smoke envía telemetría sintética por MQTT/TLS, comprueba Bronze/Silver/cuarentena/Gold,
reinicia Spark, repite el evento y ejecuta las consultas de los cinco dashboards a través
de Grafana. Para una alternativa sin broker doméstico, usar `--transport kafka`.
Las evidencias se guardan en `docs/evidence/`. La importación Parquet y la suite de
integración Iceberg están documentadas en [la guía de la semana 1](docs/SEMANA_1.md).

## Parada

```bash
docker compose down
```

Los volúmenes persistentes se conservan. `docker compose down -v` elimina el log Kafka,
el lakehouse y los catálogos: no usarlo para un reinicio normal.

## Batch meteorológico AEMET (semana 2)

Airflow está integrado en Docker Compose y disponible en http://localhost:8082.
El DAG `aemet_daily_bronze` descarga diariamente climatología de AEMET, conserva el
JSON original en Bronze MinIO, carga la estación configurada de forma idempotente en
Iceberg Silver y ejecuta dbt para publicar `tfm.gold.environment_daily`. Configurar
`AEMET_API_KEY` en `.env`. Programación, acceso, ejecución manual y venv:
[docs/AEMET_BATCH.md](docs/AEMET_BATCH.md).

## Batch eléctrico Datadis

El DAG `datadis_daily_consumption` conserva la curva horaria de los suministros eléctricos en Bronze, la normaliza en Iceberg Silver y publica el agregado diario `tfm.gold.grid_energy_daily` para el dashboard de energía. La configuración, privacidad del CUPS y operación están en [docs/DATADIS_BATCH.md](docs/DATADIS_BATCH.md).

## Detección de anomalías (semana 3)

Los datos sintéticos pueden incluir anomalías etiquetadas y el DAG
`anomaly_train_and_score` aplica reglas explicables e Isolation Forest para publicarlas
en `tfm.iot_gold.anomaly_events`. La configuración, ejecución y consultas de
validación están en [docs/SEMANA_3.md](docs/SEMANA_3.md).
