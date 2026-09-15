select * from {{ source('iot', 'datadis_consumption_hourly') }}
where consumption_kwh is not null
