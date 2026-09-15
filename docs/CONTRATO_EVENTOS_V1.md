# Contrato común IoT v1

Implementación normativa: `spark-lakehouse/event_contract.py`. Las pruebas ejercitan
esta misma función en Python y el job la ejecuta como UDF de Spark. Todos los tiempos
de las tablas se interpretan en UTC con precisión de microsegundos.

## Entrada

```json
{
  "schema_version": 1,
  "source": "synthetic",
  "entity_id": "sensor.example_temperature",
  "domain": "sensor",
  "state": "23.4",
  "event_timestamp": "2026-09-13T12:00:00+00:00",
  "updated_timestamp": "2026-09-13T12:00:00+00:00",
  "ingestion_timestamp": "2026-09-13T12:00:01+00:00",
  "attributes": {
    "device_class": "temperature",
    "unit_of_measurement": "°C",
    "area_id": "example_room",
    "friendly_name": "Example temperature",
    "synthetic": true
  }
}
```

`schema_version` omitida equivale a 1 para admitir los eventos existentes. Se exige
un objeto JSON. Los campos obligatorios son `source`, `entity_id`, `domain`, `state`
y `event_timestamp`. Source admite `matter`, `homekit`, `synthetic`. Dominio y prefijo
de entidad deben coincidir. Se aceptan estados textuales y números JSON finitos;
los atributos son un objeto JSON y pueden contener estructuras anidadas.

## Columnas comunes en Bronze, Silver y cuarentena

| Grupo | Campos y semántica |
|---|---|
| Identidad | `event_id`: SHA-256 del contenido estable del evento; `observation_id`: SHA-256 de identidad del log, topic, partición y offset |
| Origen | `source`, `entity_id`, `domain`, `entity_key` (clave de Kafka) |
| Medida | `state` textual; `numeric_state` double finito cuando se puede convertir; `unit`, `device_class` |
| Contexto | `area_id`, `friendly_name`, `attributes` como mapa string/string, `attributes_json` como JSON canónico sin perder tipos anidados |
| Tiempo del dispositivo | `event_time`, `updated_time`, más los textos originales `event_timestamp`, `updated_timestamp` |
| Transporte | `ingestion_time`, `bridge_time`, `kafka_timestamp`; textos originales de ingesta y puente; `mqtt_topic`, topic/partición/offset de Kafka |
| Procesamiento | `bronze_timestamp`, `processing_time`, `event_date` derivada del evento |
| Contrato | `schema_version=1`, `quality_rules_version=quality-v1`, `quality_config` con umbrales aplicados |
| Calidad | `quality_status` (`valid`, `warning`, `quarantined`), `quality_flag` resumen, `quality_errors` y `quality_warnings` con todas las causas |
| Auditoría | `raw_value`: contenido recibido de Kafka, incluidos los payloads malformados |

Los atributos no presentes no se inventan: por ejemplo, `area_id` queda nula si el
origen no la facilita. Los valores anidados se serializan como JSON canónico en el
mapa compatible con Grafana. `attributes_json` y `raw_value` permiten reconstruirlos.
El ingestor añade tópico MQTT y tiempo de puente a los objetos JSON válidos. El
crudo Bronze es el mensaje Kafka resultante, no una copia byte a byte del MQTT previo.
Para payloads no JSON se conserva el contenido sin envolverlo; la identidad de
transporte queda en Kafka. El UTF-8 inválido puede convertirse a caracteres de
sustitución al convertir los bytes Kafka a string; no se promete conservación binaria.

## Identidad y deduplicación

La entrada al hash de `event_id` es el array JSON canónico:

```
[1, source, entity_id, domain, state,
 event_timestamp_utc, updated_timestamp_utc, attributes]
```

Se ordenan las claves del JSON y se normalizan timestamps con zona a UTC. Se eliminan
espacios periféricos de los campos principales. No se incluyen ingesta, puente,
procesamiento, tópico MQTT, partición ni offset. No se confía en un `event_id`
aportado por el emisor. Un cambio real de atributos o de `updated_timestamp` produce
otro evento, aunque `last_changed` y el estado sigan iguales. Representaciones
textuales diferentes de un estado (`"23.4"` y `"23.40"`) se consideran eventos distintos.

Bronze conserva cada entrega Kafka una sola vez por `observation_id`, incluso tras
releer el log o volver a importar Parquet. Silver usa `MERGE ... WHEN NOT MATCHED`
por `event_id`, eligiendo una entrega por ID antes del MERGE. La primera ya confirmada
en Silver conserva sus metadatos de transporte. La cuarentena conserva cada entrega
inválida por `observation_id`. Los reenvíos quedan auditados en la vista de duplicados.

`KAFKA_LOG_ID` identifica la vida del log: cambiarlo si se destruye/recrea el topic y
se reutilizan offsets. Mantenerlo al reiniciar contenedores o durante la migración.
El sistema requiere **un único escritor de producción**; Iceberg no impone una
restricción UNIQUE y dos jobs independientes no constituyen una configuración soportada.

## Reglas de calidad

| Regla | Resultado |
|---|---|
| JSON malformado / raíz distinta de objeto | `invalid_json` / `invalid_payload_type` y causas de campos ausentes |
| Origen ausente / desconocido | `missing_source` / `invalid_source` |
| Entidad ausente / formato inválido | `missing_entity` / `invalid_entity` |
| Dominio ausente / incoherente | `missing_domain` / `domain_mismatch` |
| Estado ausente / tipo inválido | `missing_state` / `invalid_state` |
| `unknown`, `unavailable` (sin distinguir mayúsculas) | `state_unknown`, `state_unavailable` |
| Fecha del evento ausente / inválida / sin zona | `missing_event_time` / `invalid_event_time` |
| Fecha opcional presente e inválida | `invalid_updated_time`, `invalid_ingestion_time`, `invalid_bridge_time` |
| Versión diferente de 1 / atributos no objeto | `unsupported_schema_version` / `invalid_attributes` |
| Número no finito / medida ambiental no numérica | `non_finite_state` / `invalid_numeric_state` |
| Temperatura fuera de límites en Celsius | `temperature_out_of_range` |
| Humedad fuera de [0,100] % | `humidity_out_of_range` |
| Unidad ambiental ausente/no soportada | `unsupported_temperature_unit`, `unsupported_humidity_unit` |
| Evento posterior a Kafka más de la tolerancia | `future_event` |
| Evento anterior a Kafka más del umbral | **Advertencia** `late_event`: permanece en Silver |
| Segunda y posteriores entregas del mismo evento | `duplicate_event` en vistas de auditoría; no duplica Silver |
| Sensor sin recepción durante la ventana | `inactive` en `sensor_activity`; si nunca recibió, `never_seen` |

Las reglas de rango ambiental usan `device_class`; un sensor de batería con unidad
`%` no se confunde con humedad. Para temperatura se aceptan °C/C, °F/F y K, convirtiendo
solo para validar el rango; el valor y la unidad originales permanecen en las tablas.
Las fechas inválidas no se sustituyen por la fecha Kafka para hacer válido un evento.
La partición física usa el día de recepción Kafka, incluso si el tiempo del evento
es nulo o histórico. Gold se particiona por el día de su ventana.

| Variable de entorno | Predeterminado |
|---|---:|
| `QUALITY_TEMPERATURE_MIN_C` | -40 |
| `QUALITY_TEMPERATURE_MAX_C` | 85 |
| `QUALITY_FUTURE_TOLERANCE_SECONDS` | 300 |
| `QUALITY_LATE_THRESHOLD_SECONDS` | 86400 |
| `QUALITY_INACTIVITY_SECONDS` | 86400 |

La comparación temporal usa `kafka_timestamp` como referencia estable, no el reloj
del reintento. Kafka utiliza CreateTime en este entorno: es un timestamp del registro
Kafka, no una prueba independiente de la hora de llegada al broker. Un productor
que lo falsee puede alterar la evaluación temporal; se presupone el log de productores
locales autorizado. No hay watermark que descarte históricos. Los umbrales aplicados
quedan guardados por observación; un reintento usa la clasificación Bronze confirmada.
Cambiar umbrales afecta a nuevas observaciones; reclasificar el histórico requiere
un proceso explícito versionado, fuera del arranque normal.

La inactividad se evalúa al consultar Trino, tomando la última recepción Kafka e
incluyendo el inventario conocido. No depende de recibir otro evento para cambiar
de estado. En Home Assistant hay entidades que publican solo al cambiar; inactividad
significa ausencia de eventos y no prueba por sí sola un fallo del dispositivo.

## Consistencia entre capas

Cada tabla tiene commit atómico, pero Bronze, Silver, cuarentena y Gold no comparten
una transacción. El checkpoint de streaming avanza al completar las cuatro escrituras.
Si hay un fallo parcial, el microbatch se repite y los MERGE convergen sin duplicar.
Gold recalcula las ventanas afectadas desde Silver ya confirmada, incluida una ventana
antigua; cuenta solo eventos aceptados y únicos. Durante un microbatch puede haber
desfase entre capas. `valid_event_count = event_count` en Gold porque la cuarentena
se excluye. No se interpreta igualdad Bronze/Silver como condición de calidad.
