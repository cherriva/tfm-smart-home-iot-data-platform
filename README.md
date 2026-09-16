# TFM Smart Home IoT

Plataforma local de datos IoT para recoger telemetría de un hogar, validarla,
almacenarla en un lakehouse Iceberg y convertirla en métricas consultables desde
Trino y Grafana. Incluye datos reales de Home Assistant mediante Matter sobre Zigbee y un generador
sintético reproducible y cargas batch de AEMET y Datadis.

La documentación funcional y operativa está en [docs](docs/).
El modelo analítico consumido por Grafana se describe en
[docs/GOLD_ANALYTICS.md](docs/GOLD_ANALYTICS.md).

El lakehouse usa Apache Iceberg, Silver idempotente y calidad auditable. El contrato
de eventos está definido en [docs/CONTRATO_EVENTOS_V1.md](docs/CONTRATO_EVENTOS_V1.md).

Canalización ejecutable:

```text
Home Assistant (Matter sobre Zigbee) -> MQTT/TLS -> mqtt-ingestor -> Redpanda -> Spark -> MinIO Iceberg Bronze/Silver/Gold -> Trino -> Grafana
                                                        ^
                                             synthetic-generator
```

### Componentes

| Componente | Función |
|---|---|
| `mqtt-ingestor` | Consume MQTT/TLS, normaliza identificadores y publica eventos en Redpanda. |
| `synthetic-generator` | Genera histórico y nuevas muestras cada 5 minutos para demos y pruebas. |
| Redpanda | Broker Kafka que desacopla productores y procesamiento. |
| `spark-lakehouse` | Valida el contrato, deduplica, envía inválidos a cuarentena y actualiza Iceberg. |
| MinIO + Hive Metastore | Almacenamiento S3 local y catálogo persistente de tablas Iceberg. |
| Airflow + dbt | Orquestan AEMET, Datadis y modelos batch Gold. |
| Trino | Consulta Iceberg y publica vistas analíticas estables. |
| Grafana | Visualiza exclusivamente marts Gold mediante dashboards provisionados. |

### Estructura del repositorio

```text
airflow/                  DAGs y jobs batch       dbt/                     modelos Gold
grafana/                  dashboards y provisión  hive/                    metastore
mqtt-ingestor/            MQTT → Redpanda         spark-lakehouse/          streaming Iceberg
synthetic-generator/      datos de demostración   trino/                   catálogos y vistas SQL
scripts/                  validación y operación  tests/                   pruebas de contratos
docs/                     decisiones y evidencias
```

`matter_inventory.csv` se monta en Spark y en el generador, pero está ignorado por
Git porque puede contener inventario doméstico. Cada instalación debe proporcionar
su copia local con las columnas del inventario usado en desarrollo.

## Arranque

1. El stack no necesita un entorno Python local: Docker construye las imágenes y
   contiene las dependencias de ingesta, Spark y Airflow. El entorno virtual sólo es
   opcional para ejecutar tests y scripts desde el host:

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
- Filtro MQTT de entrada: `tfm/matter/events/#`.
- Tópico de destino en Redpanda: `tfm.matter.events`.
- El broker Kafka se publica localmente en `localhost:19092`.
- El ingestor usa internamente `redpanda:9092`; no hace falta exponer Kafka a Internet.
- `synthetic-generator` crea un histórico de demostración desde 40 días antes hasta el
  momento de ejecución y continúa generando datos cada 5 minutos para todas las entidades.
- `spark-lakehouse` escribe tablas Iceberg v2 en `s3a://tfm-lakehouse` y usa el checkpoint `s3a://tfm-checkpoints/iceberg_v1`.
- Trino expone las tablas `tfm.bronze.matter_events`, `tfm.silver.matter_events` y `tfm.gold.entity_5m` por SQL en http://localhost:8081.

Para consumir una muestra desde el contenedor de Redpanda:

```bash
docker compose exec redpanda rpk topic consume tfm.matter.events --brokers redpanda:9092 -n 1
```

El ingestor conserva el JSON de Home Assistant y añade `mqtt_topic` y `bridge_timestamp`.

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
Iceberg gestiona sus particiones y
snapshots sin procesos auxiliares de sincronización. Las vistas compatibles conservan
los nombres usados por Grafana.
Las tablas físicas son `tfm.iot_bronze.matter_events`, `tfm.iot_silver.matter_events`,
`tfm.iot_gold.entity_5m` y `tfm.iot_quality.quarantine_events`.

Grafana se provisiona automáticamente con el plugin de Trino, la fuente de datos `Trino`
y once dashboards. `TFM Smart Home` contiene la visión general y los dominios de confort,
energía, ocupación, aire, seguridad, agua, HVAC y garaje. `TFM Platform` contiene calidad
del dato e ingesta por capas. Las definiciones se revisan cada 30 segundos y consultan
exclusivamente marts Gold. El usuario inicial es `admin` y la contraseña se toma de
`GRAFANA_ADMIN_PASSWORD`.

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
reinicia Spark, repite el evento y ejecuta las consultas de los dashboards a través
de Grafana. Para una alternativa sin broker doméstico, usar `--transport kafka`.
Las evidencias se guardan en `docs/evidence/`. La importación Parquet y la suite de
integración Iceberg están documentadas en [docs/CONTRATO_EVENTOS_V1.md](docs/CONTRATO_EVENTOS_V1.md).

Para validar la configuración antes de arrancar:

```bash
docker compose config --quiet
```

### Tests automatizados

La suite local valida el contrato de eventos y sus reglas de calidad, las
transformaciones batch de AEMET y Datadis, los dashboards de Grafana, la
normalización MQTT, la reproducibilidad del generador sintético y la configuración
de Docker Compose. No necesita levantar toda la plataforma para ejecutarse:

```bash
.venv/bin/python -m unittest discover -s tests -v
```

Actualmente incluye 45 pruebas. También comprueba que no se reintroduzcan
integraciones o configuraciones retiradas y que los dashboards sólo consulten la
capa Gold.

Como validación de integración completa, el smoke test recorre el flujo real de
mensajería y lakehouse: publica un evento, comprueba Bronze, Silver, cuarentena y
Gold, reinicia Spark, repite el evento para verificar la idempotencia y consulta
los dashboards a través de Grafana:

```bash
.venv/bin/python scripts/smoke_test.py --transport kafka --restart
```

La variante MQTT utiliza el broker TLS configurado en `.env`:

```bash
.venv/bin/python scripts/smoke_test.py --restart
```

## Parada

```bash
docker compose down
```

Los volúmenes persistentes se conservan. `docker compose down -v` elimina el log Kafka,
el lakehouse y los catálogos: no usarlo para un reinicio normal.

## Batch meteorológico AEMET

Airflow está integrado en Docker Compose y disponible en http://localhost:8082.
El DAG `aemet_daily_bronze` descarga diariamente climatología de AEMET, conserva el
JSON original en Bronze MinIO, carga la estación configurada de forma idempotente en
Iceberg Silver y ejecuta dbt para publicar `tfm.gold.environment_daily`. Configurar
`AEMET_API_KEY` en `.env`. Programación, acceso y ejecución manual:
[docs/AEMET_BATCH.md](docs/AEMET_BATCH.md).

## Batch eléctrico Datadis

El DAG `datadis_daily_consumption` conserva la curva horaria de los suministros eléctricos en Bronze, la normaliza en Iceberg Silver y publica el agregado diario `tfm.gold.grid_energy_daily` para el dashboard de energía. La configuración, privacidad del CUPS y operación están en [docs/DATADIS_BATCH.md](docs/DATADIS_BATCH.md).
