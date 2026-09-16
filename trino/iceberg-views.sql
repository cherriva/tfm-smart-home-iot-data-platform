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

-- Centralized business thresholds. Dashboards consume Gold metrics and never
-- duplicate these rules in panel SQL.
CREATE OR REPLACE VIEW tfm.reference.analytics_thresholds AS
SELECT * FROM (VALUES (
    18.0, 26.0, 30.0, 70.0, 1000.0, 15.0, 0.25, 21.0
)) AS t(
    comfort_temperature_min_c,
    comfort_temperature_max_c,
    comfort_humidity_min_pct,
    comfort_humidity_max_pct,
    healthy_co2_max_ppm,
    expected_sample_minutes,
    electricity_price_eur_kwh,
    default_hvac_target_temperature_c
);

CREATE OR REPLACE VIEW tfm.iot_gold.room_daily AS
SELECT CAST(time AS date) AS day, room,
       avg(temperature_c) AS avg_temperature_c,
       min(temperature_c) AS min_temperature_c,
       max(temperature_c) AS max_temperature_c,
       avg(humidity_pct) AS avg_humidity_pct,
       avg(co2_ppm) AS avg_co2_ppm,
       max(co2_ppm) AS max_co2_ppm,
       count_if(temperature_c IS NOT NULL) AS temperature_hours,
       count_if(humidity_pct IS NOT NULL) AS humidity_hours,
       count_if(co2_ppm IS NOT NULL) AS co2_hours
FROM tfm.iot_gold.room_hourly
GROUP BY 1, 2;

CREATE OR REPLACE VIEW tfm.iot_gold.comfort_hourly AS
SELECT r.time, r.room, r.temperature_c, r.humidity_pct, r.co2_ppm,
       a.mean_temperature_c AS exterior_temperature_c,
       CASE WHEN r.temperature_c BETWEEN t.comfort_temperature_min_c AND t.comfort_temperature_max_c
                  AND r.humidity_pct BETWEEN t.comfort_humidity_min_pct AND t.comfort_humidity_max_pct
            THEN 100.0 ELSE 0.0 END AS comfort_score,
       CASE WHEN r.temperature_c BETWEEN t.comfort_temperature_min_c AND t.comfort_temperature_max_c
                  AND r.humidity_pct BETWEEN t.comfort_humidity_min_pct AND t.comfort_humidity_max_pct
            THEN true ELSE false END AS is_comfortable
FROM tfm.iot_gold.room_hourly r
CROSS JOIN tfm.reference.analytics_thresholds t
LEFT JOIN tfm.iot_silver.aemet_daily a ON a.observation_date = CAST(r.time AS date);

CREATE OR REPLACE VIEW tfm.iot_gold.comfort_daily AS
SELECT CAST(time AS date) AS day, room,
       avg(comfort_score) AS comfort_ratio_pct,
       count_if(NOT is_comfortable) AS hours_outside_comfort,
       avg(temperature_c) AS avg_temperature_c,
       min(temperature_c) AS min_temperature_c,
       max(temperature_c) AS max_temperature_c,
       avg(humidity_pct) AS avg_humidity_pct,
       avg(exterior_temperature_c) AS exterior_temperature_c
FROM tfm.iot_gold.comfort_hourly
GROUP BY 1, 2;

CREATE OR REPLACE VIEW tfm.iot_gold.home_daily AS
SELECT day,
       avg(avg_temperature_c) AS avg_temperature_c,
       max(max_temperature_c) AS hottest_temperature_c,
       min(min_temperature_c) AS coldest_temperature_c,
       max(max_temperature_c) - min(min_temperature_c) AS thermal_spread_c,
       avg(avg_humidity_pct) AS avg_humidity_pct,
       avg(avg_co2_ppm) AS avg_co2_ppm,
       max(max_co2_ppm) AS max_co2_ppm
FROM tfm.iot_gold.room_daily
GROUP BY day;

CREATE OR REPLACE VIEW tfm.iot_gold.energy_hourly AS
WITH per_entity AS (
    SELECT date_trunc('hour', event_time) AS time, source,
           coalesce(nullif(area_id, ''), 'sin_area') AS room, entity_id,
           avg(numeric_state) AS avg_power_w
    FROM tfm.iot_silver.matter_events
    WHERE unit = 'W' AND numeric_state IS NOT NULL
    GROUP BY 1, 2, 3, 4
)
SELECT time, source, room, sum(avg_power_w) / 1000.0 AS avg_power_kw,
       sum(avg_power_w) / 1000.0 AS energy_kwh
FROM per_entity
GROUP BY 1, 2, 3;

CREATE OR REPLACE VIEW tfm.iot_gold.energy_daily AS
WITH home_hourly AS (
    SELECT time, source, sum(energy_kwh) AS energy_kwh,
           sum(avg_power_kw) AS home_power_kw
    FROM tfm.iot_gold.energy_hourly
    GROUP BY 1, 2
), estimated AS (
    SELECT CAST(time AS date) AS day, source,
           sum(energy_kwh) AS estimated_energy_kwh,
           max(home_power_kw) AS peak_power_kw,
           approx_percentile(home_power_kw, 0.10) AS baseline_power_kw
    FROM home_hourly
    GROUP BY 1, 2
), real_grid AS (
    SELECT reading_date AS day, sum(consumption_kwh) AS grid_energy_kwh
    FROM tfm.iot_silver.datadis_consumption_hourly
    GROUP BY 1
)
SELECT coalesce(e.day, g.day) AS day, e.source, e.estimated_energy_kwh,
       g.grid_energy_kwh,
       g.grid_energy_kwh * t.electricity_price_eur_kwh AS estimated_cost_eur,
       e.peak_power_kw, e.baseline_power_kw
FROM estimated e
FULL OUTER JOIN real_grid g ON e.day = g.day AND e.source = 'synthetic'
CROSS JOIN tfm.reference.analytics_thresholds t;

CREATE OR REPLACE VIEW tfm.iot_gold.energy_room_daily AS
SELECT CAST(time AS date) AS day, room, sum(energy_kwh) AS estimated_energy_kwh,
       max(avg_power_kw) AS peak_power_kw
FROM tfm.iot_gold.energy_hourly
WHERE source = 'synthetic'
GROUP BY 1, 2;

CREATE OR REPLACE VIEW tfm.iot_gold.occupancy_hourly AS
WITH signals AS (
    SELECT date_trunc('hour', event_time) AS time,
           coalesce(nullif(area_id, ''), 'sin_area') AS room,
           count_if((device_class IN ('occupancy', 'presence') OR entity_id LIKE '%presence%')
                    AND lower(state) IN ('on', 'home', 'occupied', 'true')) AS occupied_samples,
           count_if(domain = 'light' AND lower(state) = 'on') AS light_on_samples,
           count_if(device_class IN ('occupancy', 'presence') OR entity_id LIKE '%presence%') AS presence_samples
    FROM tfm.iot_silver.matter_events
    GROUP BY 1, 2
)
SELECT time, room,
       least(60.0, occupied_samples * t.expected_sample_minutes) AS occupied_minutes,
       least(60.0, light_on_samples * t.expected_sample_minutes) AS light_on_minutes,
       CASE WHEN occupied_samples = 0
            THEN least(60.0, light_on_samples * t.expected_sample_minutes) ELSE 0.0 END AS light_without_presence_minutes,
       occupied_samples > 0 AS occupied
FROM signals CROSS JOIN tfm.reference.analytics_thresholds t
WHERE presence_samples > 0 OR light_on_samples > 0;

CREATE OR REPLACE VIEW tfm.iot_gold.occupancy_daily AS
SELECT CAST(time AS date) AS day, room,
       sum(occupied_minutes) / 60.0 AS occupied_hours,
       sum(light_on_minutes) / 60.0 AS light_on_hours,
       sum(light_without_presence_minutes) / 60.0 AS light_without_presence_hours,
       CASE WHEN sum(light_on_minutes) > 0
            THEN 100.0 * (1.0 - sum(light_without_presence_minutes) / sum(light_on_minutes)) END AS light_efficiency_pct
FROM tfm.iot_gold.occupancy_hourly
GROUP BY 1, 2;

CREATE OR REPLACE VIEW tfm.iot_gold.air_quality_hourly AS
SELECT c.time, c.room, c.co2_ppm, c.humidity_pct, c.exterior_temperature_c,
       CASE WHEN c.co2_ppm IS NULL THEN NULL
            WHEN c.co2_ppm <= t.healthy_co2_max_ppm THEN 100.0 ELSE 0.0 END AS healthy_air_score,
       c.co2_ppm > t.healthy_co2_max_ppm AS co2_over_threshold
FROM tfm.iot_gold.comfort_hourly c
CROSS JOIN tfm.reference.analytics_thresholds t;

CREATE OR REPLACE VIEW tfm.iot_gold.air_quality_daily AS
SELECT CAST(time AS date) AS day, room, avg(co2_ppm) AS avg_co2_ppm,
       max(co2_ppm) AS max_co2_ppm, avg(humidity_pct) AS avg_humidity_pct,
       avg(healthy_air_score) AS healthy_air_ratio_pct,
       count_if(co2_over_threshold) AS hours_over_co2_threshold
FROM tfm.iot_gold.air_quality_hourly
GROUP BY 1, 2;

CREATE OR REPLACE VIEW tfm.iot_gold.access_events AS
WITH ordered AS (
    SELECT event_time, source, entity_id,
           coalesce(nullif(area_id, ''), 'sin_area') AS room,
           device_class, state,
           lag(state) OVER (PARTITION BY source, entity_id ORDER BY event_time) AS previous_state
    FROM tfm.iot_silver.matter_events
    WHERE domain IN ('binary_sensor', 'lock')
      AND (device_class IN ('door', 'window', 'opening', 'moisture')
           OR entity_id LIKE '%door%' OR entity_id LIKE '%puerta%'
           OR entity_id LIKE '%window%' OR entity_id LIKE '%ventana%'
           OR entity_id LIKE '%lock%' OR entity_id LIKE '%leak%')
), transitions AS (
    SELECT * FROM ordered WHERE previous_state IS NULL OR state <> previous_state
), timed AS (
    SELECT *, lead(event_time) OVER (PARTITION BY source, entity_id ORDER BY event_time) AS next_event_time
    FROM transitions
)
SELECT event_time, source, entity_id, room, device_class, state,
       CASE WHEN lower(state) IN ('on', 'open', 'unlocked', 'wet') THEN 'OPEN' ELSE 'CLOSED' END AS access_state,
       CASE WHEN previous_state IS NULL THEN 'initial'
            WHEN lower(state) IN ('on', 'open', 'unlocked', 'wet') THEN 'opened' ELSE 'closed' END AS event_type,
       CASE WHEN lower(state) IN ('on', 'open', 'unlocked', 'wet')
            THEN date_diff('second', event_time, next_event_time) / 60.0 END AS open_duration_minutes
FROM timed;

CREATE OR REPLACE VIEW tfm.iot_gold.water_daily AS
WITH samples AS (
    SELECT event_time, CAST(event_time AS date) AS day,
           coalesce(nullif(area_id, ''), 'sin_area') AS room, entity_id, numeric_state,
           lead(event_time) OVER (PARTITION BY source, entity_id ORDER BY event_time) AS next_event_time
    FROM tfm.iot_silver.matter_events
    WHERE (device_class = 'water' OR unit = 'L/min') AND numeric_state > 0
)
SELECT day, room,
       sum(numeric_state * least(15.0, greatest(0.0, date_diff('second', event_time, next_event_time) / 60.0))) AS water_liters,
       max(numeric_state) AS peak_flow_l_min,
       count_if(numeric_state > 0) AS active_samples
FROM samples
GROUP BY 1, 2;

CREATE OR REPLACE VIEW tfm.iot_gold.shower_events AS
WITH active AS (
    SELECT event_time, coalesce(nullif(area_id, ''), 'sin_area') AS room,
           entity_id, numeric_state,
           lag(event_time) OVER (PARTITION BY entity_id ORDER BY event_time) AS previous_time,
           lead(event_time) OVER (PARTITION BY entity_id ORDER BY event_time) AS next_time
    FROM tfm.iot_silver.matter_events
    WHERE (device_class = 'water' OR unit = 'L/min') AND numeric_state > 0
), marked AS (
    SELECT *, CASE WHEN previous_time IS NULL OR date_diff('minute', previous_time, event_time) > 20 THEN 1 ELSE 0 END AS new_session
    FROM active
), grouped AS (
    SELECT *, sum(new_session) OVER (PARTITION BY entity_id ORDER BY event_time) AS shower_id
    FROM marked
)
SELECT room, entity_id, shower_id, min(event_time) AS shower_start,
       max(coalesce(next_time, event_time)) AS shower_end,
       date_diff('second', min(event_time), max(coalesce(next_time, event_time))) / 60.0 AS duration_minutes,
       sum(numeric_state * least(15.0, greatest(0.0, date_diff('second', event_time, next_time) / 60.0))) AS water_liters,
       max(numeric_state) AS peak_flow_l_min
FROM grouped
GROUP BY 1, 2, 3;

CREATE OR REPLACE VIEW tfm.iot_gold.hvac_hourly AS
SELECT date_trunc('hour', event_time) AS time,
       coalesce(nullif(area_id, ''), 'sin_area') AS room,
       sum(CASE WHEN lower(state) IN ('heat', 'heating', 'cool', 'cooling', 'on') THEN 15.0 ELSE 0.0 END) AS hvac_active_minutes,
       sum(CASE WHEN lower(state) IN ('heat', 'heating') THEN 15.0 ELSE 0.0 END) AS heating_minutes,
       sum(CASE WHEN lower(state) IN ('cool', 'cooling') THEN 15.0 ELSE 0.0 END) AS cooling_minutes,
       avg(coalesce(
           try_cast(element_at(attributes, 'temperature') AS double),
           try_cast(element_at(attributes, 'target_temperature') AS double),
           t.default_hvac_target_temperature_c
       )) AS target_temperature_c
FROM tfm.iot_silver.matter_events
CROSS JOIN tfm.reference.analytics_thresholds t
WHERE domain = 'climate'
GROUP BY 1, 2;

CREATE OR REPLACE VIEW tfm.iot_gold.hvac_daily AS
SELECT CAST(time AS date) AS day, room,
       sum(hvac_active_minutes) / 60.0 AS hvac_hours,
       sum(heating_minutes) / 60.0 AS heating_hours,
       sum(cooling_minutes) / 60.0 AS cooling_hours,
       avg(target_temperature_c) AS target_temperature_c
FROM tfm.iot_gold.hvac_hourly
GROUP BY 1, 2;

CREATE OR REPLACE VIEW tfm.iot_gold.garage_events AS
SELECT event_time, entity_id, access_state AS state, event_type,
       open_duration_minutes AS duration_minutes
FROM tfm.iot_gold.access_events
WHERE room = 'garaje'
UNION ALL
SELECT event_time, entity_id,
       CASE WHEN lower(state) IN ('on', 'home', 'present', 'true') THEN 'PRESENT' ELSE 'ABSENT' END,
       CASE WHEN lower(state) IN ('on', 'home', 'present', 'true') THEN 'vehicle_arrival' ELSE 'vehicle_departure' END,
       CAST(NULL AS double)
FROM (
    SELECT event_time, entity_id, state,
           lag(state) OVER (PARTITION BY source, entity_id ORDER BY event_time) AS previous_state
    FROM tfm.iot_silver.matter_events
    WHERE area_id = 'garaje' AND (entity_id LIKE '%vehicle%' OR device_class = 'presence')
) vehicle
WHERE previous_state IS NULL OR state <> previous_state;

CREATE OR REPLACE VIEW tfm.iot_gold.sensor_health AS
SELECT source, entity_id, domain, area_id, last_received, last_event_time,
       seconds_since_received / 60.0 AS minutes_since_received, activity_status,
       last_quality_flag
FROM tfm.iot_quality.sensor_activity;

CREATE OR REPLACE VIEW tfm.iot_gold.data_quality AS
SELECT date_trunc('hour', processing_time) AS time, source,
       count(*) AS received_events,
       count_if(quality_status <> 'quarantined' AND delivery_rank = 1) AS valid_events,
       count_if(quality_status = 'quarantined') AS quarantined_events,
       count_if(delivery_rank > 1) AS duplicate_events,
       100.0 * count_if(quality_status <> 'quarantined' AND delivery_rank = 1) / count(*) AS valid_ratio_pct,
       avg(date_diff('millisecond', event_time, ingestion_time)) AS avg_ingestion_latency_ms
FROM tfm.iot_quality.events
GROUP BY 1, 2;

CREATE OR REPLACE VIEW tfm.iot_gold.ingestion_summary AS
SELECT
    (SELECT count(*) FROM tfm.iot_bronze.matter_events) AS bronze_records,
    (SELECT count(*) FROM tfm.iot_silver.matter_events) AS silver_records,
    (SELECT count(*) FROM tfm.iot_gold.entity_5m) AS gold_records,
    (SELECT count(*) FROM tfm.iot_quality.quarantine_events) AS quarantine_records,
    (SELECT count(*) FROM tfm.iot_quality.duplicate_events) AS duplicate_records,
    (SELECT max(processing_time) FROM tfm.iot_bronze.matter_events) AS last_bronze_record,
    (SELECT max(processing_time) FROM tfm.iot_silver.matter_events) AS last_silver_record,
    (SELECT max(window_start) FROM tfm.iot_gold.entity_5m) AS last_gold_window,
    100.0 * (SELECT count(*) FROM tfm.iot_silver.matter_events)
        / nullif((SELECT count(*) FROM tfm.iot_bronze.matter_events), 0) AS silver_acceptance_pct,
    100.0 * (SELECT count(*) FROM tfm.iot_gold.entity_5m)
        / nullif((SELECT count(*) FROM tfm.iot_silver.matter_events), 0) AS gold_coverage_pct;

CREATE OR REPLACE VIEW tfm.iot_gold.ingestion_hourly AS
SELECT date_trunc('hour', processing_time) AS time, 'Bronze' AS layer, count(*) AS records
FROM tfm.iot_bronze.matter_events GROUP BY 1
UNION ALL
SELECT date_trunc('hour', processing_time), 'Silver', count(*)
FROM tfm.iot_silver.matter_events GROUP BY 1
UNION ALL
SELECT date_trunc('hour', window_start), 'Gold', count(*)
FROM tfm.iot_gold.entity_5m GROUP BY 1
UNION ALL
SELECT date_trunc('hour', processing_time), 'Cuarentena', count(*)
FROM tfm.iot_quality.quarantine_events GROUP BY 1;

CREATE OR REPLACE VIEW tfm.iot_gold.ingestion_quality AS
SELECT source,
       count(*) AS bronze_records,
       count_if(quality_status <> 'quarantined' AND delivery_rank = 1) AS accepted_records,
       count_if(quality_status = 'quarantined') AS quarantined_records,
       count_if(delivery_rank > 1) AS duplicate_records,
       100.0 * count_if(quality_status <> 'quarantined' AND delivery_rank = 1)
           / count(*) AS acceptance_pct,
       max(processing_time) AS last_processed_at,
       max(event_time) AS last_event_at,
       avg(date_diff('millisecond', event_time, ingestion_time)) AS avg_source_latency_ms
FROM tfm.iot_quality.events
GROUP BY source;

CREATE OR REPLACE VIEW tfm.iot_gold.quarantine_causes AS
SELECT quality_flag, source, count(*) AS records,
       min(processing_time) AS first_seen_at,
       max(processing_time) AS last_seen_at
FROM tfm.iot_quality.quarantine_events
GROUP BY 1, 2;
