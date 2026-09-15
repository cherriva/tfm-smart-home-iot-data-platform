{{ config(alias='environment_daily') }}

with indoor as (
select
  cast(event_time as date) as observation_date, room_id, room_name,
  avg(case when measurement='temperature' then numeric_state end) as indoor_mean_temperature_c,
  min(case when measurement='temperature' then numeric_state end) as indoor_min_temperature_c,
  max(case when measurement='temperature' then numeric_state end) as indoor_max_temperature_c,
  avg(case when measurement='humidity' then numeric_state end) as indoor_mean_humidity_pct,
  min(case when measurement='humidity' then numeric_state end) as indoor_min_humidity_pct,
  max(case when measurement='humidity' then numeric_state end) as indoor_max_humidity_pct,
  count(*) as indoor_event_count, count(distinct source_entity_id) as indoor_sensor_entity_count,
  count_if(measurement='temperature') as temperature_event_count,
  count_if(measurement='humidity') as humidity_event_count,
  case when count_if(measurement='temperature') > 0 then 1.0 else 0.0 end as temperature_coverage,
  case when count_if(measurement='humidity') > 0 then 1.0 else 0.0 end as humidity_coverage,
  min(event_time) as first_indoor_event_time,
  max(event_time) as last_indoor_event_time
from {{ ref('stg_environment_events') }}
group by 1,2,3
), outdoor as (
  select observation_date, station_id, station_name, province,
    mean_temperature_c as outdoor_mean_temperature_c,
    min_temperature_c as outdoor_min_temperature_c,
    max_temperature_c as outdoor_max_temperature_c,
    mean_humidity_pct as outdoor_mean_humidity_pct,
    precipitation_mm as outdoor_precipitation_mm,
    sunshine_hours as outdoor_sunshine_hours
  from {{ ref('stg_aemet_daily') }}
)
select indoor.observation_date,
  indoor.room_id, indoor.room_name,
  indoor.indoor_mean_temperature_c, indoor.indoor_min_temperature_c,
  indoor.indoor_max_temperature_c, indoor.indoor_mean_humidity_pct,
  indoor.indoor_min_humidity_pct, indoor.indoor_max_humidity_pct,
  indoor.indoor_event_count, indoor.indoor_sensor_entity_count,
  indoor.temperature_event_count, indoor.humidity_event_count,
  indoor.temperature_coverage, indoor.humidity_coverage,
  indoor.first_indoor_event_time, indoor.last_indoor_event_time,
  outdoor.station_id as outdoor_station_id,
  outdoor.station_name as outdoor_station_name, outdoor.province as outdoor_province,
  outdoor.outdoor_mean_temperature_c, outdoor.outdoor_min_temperature_c,
  outdoor.outdoor_max_temperature_c, outdoor.outdoor_mean_humidity_pct,
  outdoor.outdoor_precipitation_mm, outdoor.outdoor_sunshine_hours,
  case when outdoor.outdoor_mean_temperature_c is not null
    then indoor.indoor_mean_temperature_c - outdoor.outdoor_mean_temperature_c end as mean_temperature_delta_c
from indoor left join outdoor
  on indoor.observation_date = outdoor.observation_date
