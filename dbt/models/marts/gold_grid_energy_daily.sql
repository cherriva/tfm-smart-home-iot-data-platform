{{ config(alias='grid_energy_daily') }}

select
  reading_date as observation_date,
  sum(consumption_kwh) as grid_consumption_kwh,
  sum(coalesce(surplus_energy_kwh, 0)) as surplus_energy_kwh,
  sum(coalesce(generation_energy_kwh, 0)) as generation_energy_kwh,
  sum(coalesce(self_consumption_energy_kwh, 0)) as self_consumption_energy_kwh,
  count(*) as reading_count,
  count(distinct cups) as supply_count,
  max(loaded_at) as loaded_at
from {{ ref('stg_datadis_consumption_hourly') }}
group by 1
