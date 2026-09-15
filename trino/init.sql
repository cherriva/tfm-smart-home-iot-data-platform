CREATE SCHEMA IF NOT EXISTS tfm_legacy.bronze;

CREATE SCHEMA IF NOT EXISTS tfm_legacy.silver;

CREATE SCHEMA IF NOT EXISTS tfm_legacy.gold;

CREATE TABLE IF NOT EXISTS tfm_legacy.bronze.matter_events (
    entity_key varchar,
    raw_value varchar,
    kafka_topic varchar,
    kafka_partition integer,
    kafka_offset bigint,
    kafka_timestamp timestamp,
    source varchar,
    entity_id varchar,
    state varchar,
    event_timestamp varchar,
    ingestion_timestamp varchar,
    updated_timestamp varchar,
    mqtt_topic varchar,
    bridge_timestamp varchar,
    attributes map(varchar, varchar),
    bronze_timestamp timestamp,
    event_date date,
    domain varchar
)
WITH (
    format = 'PARQUET',
    external_location = 's3://tfm-bronze/matter_events',
    partitioned_by = ARRAY['event_date', 'domain']
);

CREATE TABLE IF NOT EXISTS tfm_legacy.silver.matter_events (
    event_id varchar,
    entity_id varchar,
    entity_key varchar,
    state varchar,
    numeric_state double,
    unit varchar,
    device_class varchar,
    friendly_name varchar,
    attributes map(varchar, varchar),
    quality_flag varchar,
    event_time timestamp,
    ingestion_time timestamp,
    updated_time timestamp,
    bronze_timestamp timestamp,
    kafka_topic varchar,
    kafka_partition integer,
    kafka_offset bigint,
    mqtt_topic varchar,
    bridge_timestamp varchar,
    raw_value varchar,
    event_date date,
    domain varchar
)
WITH (
    format = 'PARQUET',
    external_location = 's3://tfm-silver/matter_events',
    partitioned_by = ARRAY['event_date', 'domain']
);

CREATE TABLE IF NOT EXISTS tfm_legacy.gold.entity_5m (
    window_start timestamp,
    window_end timestamp,
    entity_id varchar,
    unit varchar,
    event_count bigint,
    valid_event_count bigint,
    numeric_event_count bigint,
    avg_numeric_state double,
    min_numeric_state double,
    max_numeric_state double,
    last_event_time timestamp,
    event_date date,
    domain varchar
)
WITH (
    format = 'PARQUET',
    external_location = 's3://tfm-gold/entity_5m',
    partitioned_by = ARRAY['event_date', 'domain']
);

CALL tfm_legacy.system.sync_partition_metadata('bronze', 'matter_events', 'FULL');
CALL tfm_legacy.system.sync_partition_metadata('silver', 'matter_events', 'FULL');
CALL tfm_legacy.system.sync_partition_metadata('gold', 'entity_5m', 'FULL');
