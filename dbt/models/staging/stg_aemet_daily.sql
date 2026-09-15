select * from {{ source('iot', 'aemet_daily') }} where station_id = '9263D'
