select * from {{ source('analytical', 'environment_events') }} where quality_status <> 'quarantined'
