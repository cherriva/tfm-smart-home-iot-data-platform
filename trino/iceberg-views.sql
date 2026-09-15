-- Stable names used by the original five dashboards.
CREATE SCHEMA IF NOT EXISTS tfm.bronze;
CREATE SCHEMA IF NOT EXISTS tfm.silver;
CREATE SCHEMA IF NOT EXISTS tfm.gold;
CREATE OR REPLACE VIEW tfm.bronze.matter_events AS SELECT * FROM tfm.iot_bronze.matter_events;
CREATE OR REPLACE VIEW tfm.silver.matter_events AS SELECT * FROM tfm.iot_silver.matter_events;
CREATE OR REPLACE VIEW tfm.gold.entity_5m AS SELECT * FROM tfm.iot_gold.entity_5m;
CREATE OR REPLACE VIEW tfm.gold.anomaly_events AS SELECT * FROM tfm.iot_gold.anomaly_events;

-- All observations, including quarantine and duplicate delivery decisions.
CREATE OR REPLACE VIEW tfm.iot_quality.events AS
WITH ranked AS (
    SELECT b.*, row_number() OVER (
        PARTITION BY event_id, quality_status = 'quarantined'
        ORDER BY kafka_timestamp, kafka_topic, kafka_partition, kafka_offset
    ) AS delivery_rank
    FROM tfm.iot_bronze.matter_events b
)
SELECT observation_id, event_id, source, entity_id, domain, state, numeric_state,
       unit, device_class, area_id, friendly_name, attributes, raw_value,
       event_time, ingestion_time, updated_time, kafka_timestamp, bronze_timestamp,
       processing_time, quality_status, quality_warnings,
       CASE WHEN quality_status <> 'quarantined' AND delivery_rank > 1
            THEN 'duplicate_event' ELSE quality_flag END AS quality_flag,
       CASE WHEN delivery_rank > 1 THEN concat(quality_errors, ARRAY['duplicate_event'])
            ELSE quality_errors END AS quality_errors,
       delivery_rank, quality_rules_version, quality_config
FROM ranked;

CREATE OR REPLACE VIEW tfm.iot_quality.duplicate_events AS
SELECT * FROM tfm.iot_quality.events WHERE delivery_rank > 1;

-- Receipt activity is evaluated at query time: a quiet stream still becomes stale.
-- Inventory includes known sensors that have never emitted an event.
CREATE OR REPLACE VIEW tfm.iot_quality.sensor_activity AS
WITH known AS (
    SELECT source, entity_id, domain, area_id, inactivity_seconds
    FROM tfm.iot_quality.sensor_inventory
    UNION ALL
    SELECT DISTINCT b.source, b.entity_id, b.domain, b.area_id,
           CAST(json_extract_scalar(b.quality_config, '$.inactivity_seconds') AS bigint)
    FROM tfm.iot_bronze.matter_events b
    WHERE b.domain = 'sensor' AND b.entity_id IS NOT NULL AND b.source IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM tfm.iot_quality.sensor_inventory i
        WHERE i.source = b.source AND i.entity_id = b.entity_id
      )
), registry AS (
    SELECT source, entity_id, max(domain) domain, max(area_id) area_id,
           max(inactivity_seconds) inactivity_seconds FROM known GROUP BY 1, 2
), activity AS (
    SELECT source, entity_id, max(kafka_timestamp) last_received,
           max(event_time) last_event_time, max_by(quality_flag, kafka_timestamp) last_quality_flag
    FROM tfm.iot_bronze.matter_events WHERE domain = 'sensor' GROUP BY 1, 2
)
SELECT r.*, a.last_received, a.last_event_time, a.last_quality_flag,
       date_diff('second', a.last_received, CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp)) AS seconds_since_received,
       CASE WHEN a.last_received IS NULL THEN 'never_seen'
            WHEN date_diff('second', a.last_received, CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp)) > r.inactivity_seconds
            THEN 'inactive' ELSE 'active' END AS activity_status
FROM registry r LEFT JOIN activity a ON r.source = a.source AND r.entity_id = a.entity_id;

-- User-confirmed analytical names; raw event identity remains stable.
CREATE SCHEMA IF NOT EXISTS tfm.reference;
CREATE OR REPLACE VIEW tfm.reference.rooms AS
SELECT * FROM (VALUES
    ('room_primary', 'Habitación principal', CAST(NULL AS varchar)),
    ('room_2', 'Habitación 2', CAST(NULL AS varchar)),
    ('room_3', 'Habitación 3', CAST(NULL AS varchar)),
    ('bathroom_1', 'Baño 1', 'room_primary'),
    ('bathroom_2', 'Baño 2', 'room_2'),
    ('bathroom_3', 'Baño 3', 'room_3'),
    ('living_room', 'Salón', CAST(NULL AS varchar)),
    ('hall', 'Hall', CAST(NULL AS varchar))
) AS t(room_id, room_name, associated_bedroom_id);
CREATE OR REPLACE VIEW tfm.reference.environment_entities AS
SELECT * FROM (VALUES
    ('homekit', 'sensor.temperatura_salon', 'environment_living_room', 'living_room', 'temperature', 'living_room_temperature'),
    ('homekit', 'sensor.humedad_salon', 'environment_living_room', 'living_room', 'humidity', 'living_room_humidity'),
    ('homekit', 'sensor.temperatura_dormitorio_padres', 'environment_room_primary', 'room_primary', 'temperature', 'room_primary_temperature'),
    ('homekit', 'sensor.humedad_dormitorio_padres', 'environment_room_primary', 'room_primary', 'humidity', 'room_primary_humidity'),
    ('homekit', 'sensor.temperatura_dormitorio_borja', 'environment_room_3', 'room_3', 'temperature', 'room_3_temperature'),
    ('homekit', 'sensor.humedad_dormitorio_borja', 'environment_room_3', 'room_3', 'humidity', 'room_3_humidity'),
    ('matter', 'sensor.temperatura_bano_1', 'environment_bathroom_1', 'bathroom_1', 'temperature', 'bathroom_1_temperature'),
    ('matter', 'sensor.humedad_bano_1', 'environment_bathroom_1', 'bathroom_1', 'humidity', 'bathroom_1_humidity'),
    ('matter', 'sensor.temperatura_bano_2', 'environment_bathroom_2', 'bathroom_2', 'temperature', 'bathroom_2_temperature'),
    ('matter', 'sensor.humedad_bano_2', 'environment_bathroom_2', 'bathroom_2', 'humidity', 'bathroom_2_humidity'),
    ('matter', 'sensor.temperatura_bano_3', 'environment_bathroom_3', 'bathroom_3', 'temperature', 'bathroom_3_temperature'),
    ('matter', 'sensor.humedad_bano_3', 'environment_bathroom_3', 'bathroom_3', 'humidity', 'bathroom_3_humidity')
) AS t(source, entity_id, analytical_device_id, room_id, measurement, analytical_entity_id);
CREATE OR REPLACE VIEW tfm.silver.environment_events AS
SELECT e.event_id, e.source, e.entity_id AS source_entity_id,
       m.analytical_entity_id, m.analytical_device_id, m.room_id, r.room_name,
       m.measurement, e.numeric_state, e.unit, e.event_time, e.updated_time,
       e.ingestion_time, e.processing_time, e.quality_status, e.quality_warnings
FROM tfm.iot_silver.matter_events e
JOIN tfm.reference.environment_entities m
  ON e.source = m.source AND e.entity_id = m.entity_id
JOIN tfm.reference.rooms r ON m.room_id = r.room_id;

-- The daily environmental mart is owned by dbt as tfm.gold.environment_daily.
-- Keeping it out of this bootstrap SQL prevents two independent definitions.
