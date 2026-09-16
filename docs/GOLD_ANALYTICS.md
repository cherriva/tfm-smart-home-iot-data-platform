# Capa Gold analítica

Grafana consulta exclusivamente marts de `tfm.iot_gold`. La lógica de negocio, los
umbrales, las agregaciones y los eventos derivados se resuelven antes de la capa de
visualización.

Los umbrales configurables están centralizados en
`tfm.reference.analytics_thresholds`: confort térmico, humedad, CO₂, intervalo de
muestreo, tarifa eléctrica y consigna HVAC predeterminada.

## Marts

- Hogar y habitaciones: `home_daily`, `room_latest_state`, `room_hourly`, `room_daily`.
- Confort: `comfort_hourly`, `comfort_daily`.
- Energía: `energy_hourly`, `energy_daily`, `energy_room_daily`.
- Ocupación: `occupancy_hourly`, `occupancy_daily`.
- Aire: `air_quality_hourly`, `air_quality_daily`.
- Accesos: `access_events`.
- Agua: `water_daily`, `shower_events`.
- Climatización: `hvac_hourly`, `hvac_daily`.
- Garaje: `garage_events`.
- Gobierno del dato: `sensor_health`, `data_quality`.
- Ingesta: `ingestion_summary`, `ingestion_hourly`, `ingestion_quality`,
  `quarantine_causes`.

Las tablas Iceberg base de Gold (`entity_5m`, `room_latest_state` y `room_hourly`)
se actualizan incrementalmente desde Spark.
Los marts de dominio se publican como vistas Gold estables sobre esas tablas y Silver,
de forma que Grafana no reproduce reglas de negocio ni depende del esquema de ingesta.

Los dashboards se generan de forma reproducible con:

```bash
.venv/bin/python scripts/build_dashboards.py
```
