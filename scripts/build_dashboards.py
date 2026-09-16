"""Build the provisioned Grafana dashboards from Gold-layer contracts."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).parents[1]
MAIN = ROOT / "grafana" / "dashboards" / "main"
TECHNICAL = ROOT / "grafana" / "dashboards" / "technical"
DATASOURCE = {"type": "trino-datasource", "uid": "tfm-trino"}


def target(sql: str, table: bool = False) -> list[dict]:
    return [{"refId": "A", "rawSQL": sql, "format": 1 if table else 0}]


def field_config(unit: str | None = None, minimum=None, maximum=None) -> dict:
    defaults: dict = {}
    if unit:
        defaults["unit"] = unit
    if minimum is not None:
        defaults["min"] = minimum
    if maximum is not None:
        defaults["max"] = maximum
    return {"defaults": defaults, "overrides": []}


def build_dashboard(uid: str, title: str, stats: list[tuple], charts: list[tuple], *, days=40) -> dict:
    panels: list[dict] = []
    panel_id = 1
    for index, (panel_title, sql, unit) in enumerate(stats):
        panels.append({
            "id": panel_id,
            "type": "stat",
            "title": panel_title,
            "gridPos": {"h": 4, "w": 6, "x": (index % 4) * 6, "y": (index // 4) * 4},
            "datasource": DATASOURCE,
            "targets": target(sql, table=True),
            "fieldConfig": field_config(unit),
        })
        panel_id += 1
    chart_y = ((len(stats) + 3) // 4) * 4
    for index, (panel_title, panel_type, sql, unit) in enumerate(charts):
        panels.append({
            "id": panel_id,
            "type": panel_type,
            "title": panel_title,
            "gridPos": {"h": 8, "w": 12, "x": (index % 2) * 12, "y": chart_y + (index // 2) * 8},
            "datasource": DATASOURCE,
            "targets": target(sql, table=panel_type in {"table", "barchart", "bargauge", "xychart"}),
            "fieldConfig": field_config(unit),
        })
        panel_id += 1
    return {
        "uid": uid,
        "title": title,
        "schemaVersion": 41,
        "version": 1,
        "refresh": "5m",
        "time": {"from": f"now-{days}d", "to": "now"},
        "panels": panels,
    }


DASHBOARDS = {
    "02-comfort.json": build_dashboard(
        "tfm-comfort", "🌡 Confort Ambiental",
        [
            ("Temperatura media", "SELECT avg(temperature_c) value FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time)", "celsius"),
            ("Habitación más caliente", "SELECT room value FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time) GROUP BY room ORDER BY avg(temperature_c) DESC LIMIT 1", None),
            ("Habitación más fría", "SELECT room value FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time) GROUP BY room ORDER BY avg(temperature_c) LIMIT 1", None),
            ("Diferencia térmica máxima", "SELECT max(temperature_c)-min(temperature_c) value FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time)", "celsius"),
            ("Humedad media", "SELECT avg(humidity_pct) value FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time)", "percent"),
            ("Tiempo en confort", "SELECT avg(comfort_score) value FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time)", "percent"),
            ("Horas fuera de confort", "SELECT count_if(NOT is_comfortable) value FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time)", "h"),
        ],
        [
            ("Temperatura interior y exterior", "timeseries", "SELECT time, room metric, temperature_c value FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time) UNION ALL SELECT time, 'Exterior' metric, max(exterior_temperature_c) value FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time) GROUP BY time ORDER BY 1,2", "celsius"),
            ("Humedad por estancia", "timeseries", "SELECT time, room metric, humidity_pct value FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time) ORDER BY 1,2", "percent"),
            ("Confort por hora", "heatmap", "SELECT time, room metric, comfort_score value FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time) ORDER BY 1,2", "percent"),
            ("Ranking de habitaciones", "barchart", "SELECT room, avg(comfort_score) comfort_ratio_pct FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time) GROUP BY room ORDER BY 2 DESC", "percent"),
            ("Temperatura interior vs exterior", "xychart", "SELECT room, exterior_temperature_c, temperature_c FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time) AND exterior_temperature_c IS NOT NULL AND temperature_c IS NOT NULL", "celsius"),
        ],
    ),
    "03-energy.json": build_dashboard(
        "tfm-energy", "⚡ Energía y Coste",
        [
            ("Energía real hoy · Datadis", "SELECT coalesce(sum(grid_energy_kwh),0) value FROM tfm.iot_gold.energy_daily WHERE day=current_date", "kWh"),
            ("Energía real este mes · Datadis", "SELECT coalesce(sum(grid_energy_kwh),0) value FROM tfm.iot_gold.energy_daily WHERE date_trunc('month',day)=date_trunc('month',current_date)", "kWh"),
            ("Coste hoy", "SELECT coalesce(sum(estimated_cost_eur),0) value FROM tfm.iot_gold.energy_daily WHERE day=current_date", "currencyEUR"),
            ("Coste este mes", "SELECT coalesce(sum(estimated_cost_eur),0) value FROM tfm.iot_gold.energy_daily WHERE date_trunc('month',day)=date_trunc('month',current_date)", "currencyEUR"),
            ("Potencia estimada actual", "SELECT sum(avg_power_kw) value FROM tfm.iot_gold.energy_hourly WHERE time=(SELECT max(time) FROM tfm.iot_gold.energy_hourly)", "kW"),
            ("Pico de potencia", "SELECT max(peak_power_kw) value FROM tfm.iot_gold.energy_daily WHERE $__timeFilter(CAST(day AS timestamp))", "kW"),
            ("Consumo base", "SELECT avg(baseline_power_kw)*1000 value FROM tfm.iot_gold.energy_daily WHERE $__timeFilter(CAST(day AS timestamp))", "watt"),
            ("Última ingesta Datadis", "SELECT coalesce(CAST(max(day) FILTER (WHERE grid_energy_kwh IS NOT NULL) AS varchar),'Sin ingesta') value FROM tfm.iot_gold.energy_daily", None),
        ],
        [
            ("Perfil de potencia", "timeseries", "SELECT time, source metric, sum(avg_power_kw) value FROM tfm.iot_gold.energy_hourly WHERE $__timeFilter(time) GROUP BY 1,2 ORDER BY 1,2", "kW"),
            ("Consumo diario", "barchart", "SELECT day, coalesce(max(grid_energy_kwh),sum(estimated_energy_kwh)) energy_kwh FROM tfm.iot_gold.energy_daily WHERE $__timeFilter(CAST(day AS timestamp)) GROUP BY day ORDER BY day", "kWh"),
            ("Distribución estimada por estancia", "barchart", "SELECT room, sum(estimated_energy_kwh) energy_kwh FROM tfm.iot_gold.energy_room_daily WHERE $__timeFilter(CAST(day AS timestamp)) GROUP BY room ORDER BY 2 DESC", "kWh"),
            ("Consumo por hora del día", "heatmap", "SELECT time, CAST(hour(time) AS varchar) metric, sum(energy_kwh) value FROM tfm.iot_gold.energy_hourly WHERE $__timeFilter(time) GROUP BY 1,2 ORDER BY 1", "kWh"),
        ],
        days=30,
    ),
    "04-occupancy.json": build_dashboard(
        "tfm-occupancy", "👤 Ocupación y Uso de Espacios",
        [
            ("Tiempo ocupado", "SELECT sum(occupied_minutes)/60 value FROM tfm.iot_gold.occupancy_hourly WHERE $__timeFilter(time)", "h"),
            ("Habitación más utilizada", "SELECT room value FROM tfm.iot_gold.occupancy_hourly WHERE $__timeFilter(time) GROUP BY room ORDER BY sum(occupied_minutes) DESC LIMIT 1", None),
            ("Luz sin presencia", "SELECT sum(light_without_presence_minutes)/60 value FROM tfm.iot_gold.occupancy_hourly WHERE $__timeFilter(time)", "h"),
            ("Eficiencia de iluminación", "SELECT 100*(1-sum(light_without_presence_minutes)/nullif(sum(light_on_minutes),0)) value FROM tfm.iot_gold.occupancy_hourly WHERE $__timeFilter(time)", "percent"),
        ],
        [
            ("Ocupación por estancia", "barchart", "SELECT room, sum(occupied_minutes)/60 occupied_hours FROM tfm.iot_gold.occupancy_hourly WHERE $__timeFilter(time) GROUP BY room ORDER BY 2 DESC", "h"),
            ("Timeline de presencia", "state-timeline", "SELECT time, room metric, CASE WHEN occupied THEN 1 ELSE 0 END value FROM tfm.iot_gold.occupancy_hourly WHERE $__timeFilter(time) ORDER BY 1,2", "bool"),
            ("Uso por hora", "heatmap", "SELECT time, room metric, occupied_minutes value FROM tfm.iot_gold.occupancy_hourly WHERE $__timeFilter(time) ORDER BY 1,2", "m"),
            ("Uso de iluminación", "barchart", "SELECT room, sum(light_on_minutes)/60 light_on_hours, sum(light_without_presence_minutes)/60 without_presence_hours FROM tfm.iot_gold.occupancy_hourly WHERE $__timeFilter(time) GROUP BY room ORDER BY 2 DESC", "h"),
        ],
    ),
    "05-air-quality.json": build_dashboard(
        "tfm-air-quality", "🌬 Calidad del Aire y Ventilación",
        [
            ("CO₂ medio casa", "SELECT avg(co2_ppm) value FROM tfm.iot_gold.air_quality_hourly WHERE $__timeFilter(time)", "ppm"),
            ("Peor CO₂ actual", "SELECT max(co2_ppm) value FROM tfm.iot_gold.air_quality_hourly WHERE time=(SELECT max(time) FROM tfm.iot_gold.air_quality_hourly WHERE co2_ppm IS NOT NULL)", "ppm"),
            ("Máximo CO₂", "SELECT max(co2_ppm) value FROM tfm.iot_gold.air_quality_hourly WHERE $__timeFilter(time)", "ppm"),
            ("Horas sobre umbral", "SELECT count_if(co2_over_threshold) value FROM tfm.iot_gold.air_quality_hourly WHERE $__timeFilter(time)", "h"),
            ("Habitación peor ventilada", "SELECT room value FROM tfm.iot_gold.air_quality_hourly WHERE $__timeFilter(time) GROUP BY room ORDER BY avg(co2_ppm) DESC LIMIT 1", None),
        ],
        [
            ("CO₂ por habitación", "timeseries", "SELECT time, room metric, co2_ppm value FROM tfm.iot_gold.air_quality_hourly WHERE $__timeFilter(time) ORDER BY 1,2", "ppm"),
            ("Calidad del aire por habitación", "barchart", "SELECT room, avg(healthy_air_score) healthy_air_pct FROM tfm.iot_gold.air_quality_hourly WHERE $__timeFilter(time) GROUP BY room ORDER BY 2", "percent"),
            ("CO₂ y ocupación", "timeseries", "SELECT a.time, a.room metric, a.co2_ppm value FROM tfm.iot_gold.air_quality_hourly a WHERE $__timeFilter(a.time) ORDER BY 1,2", "ppm"),
            ("Humedad por habitación", "timeseries", "SELECT time, room metric, humidity_pct value FROM tfm.iot_gold.air_quality_hourly WHERE $__timeFilter(time) ORDER BY 1,2", "percent"),
        ],
    ),
    "06-security.json": build_dashboard(
        "tfm-security", "🚪 Accesos y Seguridad",
        [
            ("Accesos abiertos", "SELECT count(*) value FROM tfm.iot_gold.room_latest_state WHERE (device_class IN ('door','window','opening') OR entity_id LIKE '%door%' OR entity_id LIKE '%window%') AND lower(state) IN ('on','open','unlocked')", "short"),
            ("Fugas detectadas", "SELECT count(*) value FROM tfm.iot_gold.room_latest_state WHERE (device_class='moisture' OR entity_id LIKE '%leak%') AND lower(state) IN ('on','wet')", "short"),
            ("Aperturas en el periodo", "SELECT count_if(event_type='opened') value FROM tfm.iot_gold.access_events WHERE $__timeFilter(event_time)", "short"),
            ("Minutos abiertos", "SELECT sum(open_duration_minutes) value FROM tfm.iot_gold.access_events WHERE $__timeFilter(event_time)", "m"),
        ],
        [
            ("Aperturas por acceso", "barchart", "SELECT entity_id, count_if(event_type='opened') openings FROM tfm.iot_gold.access_events WHERE $__timeFilter(event_time) GROUP BY entity_id ORDER BY 2 DESC", "short"),
            ("Timeline de accesos", "state-timeline", "SELECT event_time time, entity_id metric, access_state value FROM tfm.iot_gold.access_events WHERE $__timeFilter(event_time) ORDER BY 1,2", None),
            ("Duración abierta", "barchart", "SELECT entity_id, sum(open_duration_minutes) total_minutes, max(open_duration_minutes) max_minutes FROM tfm.iot_gold.access_events WHERE $__timeFilter(event_time) GROUP BY entity_id ORDER BY 2 DESC", "m"),
            ("Eventos relevantes", "table", "SELECT event_time, room, entity_id, event_type, open_duration_minutes FROM tfm.iot_gold.access_events WHERE $__timeFilter(event_time) ORDER BY event_time DESC LIMIT 200", None),
        ],
    ),
    "07-water.json": build_dashboard(
        "tfm-water", "🚿 Agua y Baños",
        [
            ("Agua hoy", "SELECT coalesce(sum(water_liters),0) value FROM tfm.iot_gold.water_daily WHERE day=current_date", "litre"),
            ("Agua este mes", "SELECT sum(water_liters) value FROM tfm.iot_gold.water_daily WHERE date_trunc('month',day)=date_trunc('month',current_date)", "litre"),
            ("Duchas estimadas", "SELECT count(*) value FROM tfm.iot_gold.shower_events WHERE $__timeFilter(shower_start)", "short"),
            ("Duración media ducha", "SELECT avg(duration_minutes) value FROM tfm.iot_gold.shower_events WHERE $__timeFilter(shower_start)", "m"),
            ("Consumo medio ducha", "SELECT avg(water_liters) value FROM tfm.iot_gold.shower_events WHERE $__timeFilter(shower_start)", "litre"),
            ("Baño que más consume", "SELECT room value FROM tfm.iot_gold.water_daily WHERE $__timeFilter(CAST(day AS timestamp)) GROUP BY room ORDER BY sum(water_liters) DESC LIMIT 1", None),
        ],
        [
            ("Consumo de agua diario", "barchart", "SELECT day, room, water_liters FROM tfm.iot_gold.water_daily WHERE $__timeFilter(CAST(day AS timestamp)) ORDER BY day,room", "litre"),
            ("Flujo de agua", "timeseries", "SELECT shower_start time, room metric, peak_flow_l_min value FROM tfm.iot_gold.shower_events WHERE $__timeFilter(shower_start) ORDER BY 1,2", "litre/min"),
            ("Duchas detectadas", "table", "SELECT room, shower_start, duration_minutes, water_liters, peak_flow_l_min FROM tfm.iot_gold.shower_events WHERE $__timeFilter(shower_start) ORDER BY shower_start DESC", None),
        ],
    ),
    "08-hvac.json": build_dashboard(
        "tfm-hvac", "🔥 Climatización y Eficiencia",
        [
            ("Horas HVAC", "SELECT sum(hvac_active_minutes)/60 value FROM tfm.iot_gold.hvac_hourly WHERE $__timeFilter(time)", "h"),
            ("Horas calefacción", "SELECT sum(heating_minutes)/60 value FROM tfm.iot_gold.hvac_hourly WHERE $__timeFilter(time)", "h"),
            ("Horas refrigeración", "SELECT sum(cooling_minutes)/60 value FROM tfm.iot_gold.hvac_hourly WHERE $__timeFilter(time)", "h"),
            ("Consigna media", "SELECT avg(target_temperature_c) value FROM tfm.iot_gold.hvac_hourly WHERE $__timeFilter(time)", "celsius"),
        ],
        [
            ("Temperatura y consigna", "timeseries", "SELECT c.time, c.room metric, c.temperature_c value FROM tfm.iot_gold.comfort_hourly c WHERE $__timeFilter(c.time) UNION ALL SELECT h.time, concat(h.room,' · consigna') metric, h.target_temperature_c value FROM tfm.iot_gold.hvac_hourly h WHERE $__timeFilter(h.time) ORDER BY 1,2", "celsius"),
            ("Estado HVAC", "state-timeline", "SELECT time, room metric, CASE WHEN heating_minutes>0 THEN 'HEATING' WHEN cooling_minutes>0 THEN 'COOLING' WHEN hvac_active_minutes>0 THEN 'ON' ELSE 'OFF' END value FROM tfm.iot_gold.hvac_hourly WHERE $__timeFilter(time) ORDER BY 1,2", None),
            ("Tiempo por habitación", "barchart", "SELECT room, sum(heating_minutes)/60 heating_hours, sum(cooling_minutes)/60 cooling_hours FROM tfm.iot_gold.hvac_hourly WHERE $__timeFilter(time) GROUP BY room ORDER BY sum(hvac_active_minutes) DESC", "h"),
        ],
    ),
    "09-garage.json": build_dashboard(
        "tfm-garage", "🚗 Garaje",
        [
            ("Vehículo presente", "SELECT state value FROM tfm.iot_gold.garage_events WHERE event_type IN ('vehicle_arrival','vehicle_departure') ORDER BY event_time DESC LIMIT 1", None),
            ("Puerta del garaje", "SELECT state value FROM tfm.iot_gold.garage_events WHERE entity_id LIKE '%door%' OR entity_id LIKE '%puerta%' ORDER BY event_time DESC LIMIT 1", None),
            ("Aperturas", "SELECT count_if(event_type='opened') value FROM tfm.iot_gold.garage_events WHERE $__timeFilter(event_time)", "short"),
            ("Tiempo puerta abierta", "SELECT sum(duration_minutes) value FROM tfm.iot_gold.garage_events WHERE $__timeFilter(event_time)", "m"),
        ],
        [
            ("Vehículo y puerta", "state-timeline", "SELECT event_time time, entity_id metric, state value FROM tfm.iot_gold.garage_events WHERE $__timeFilter(event_time) ORDER BY 1,2", None),
            ("Llegadas y salidas", "barchart", "SELECT CAST(event_time AS date) day, count_if(event_type='vehicle_arrival') arrivals, count_if(event_type='vehicle_departure') departures FROM tfm.iot_gold.garage_events WHERE $__timeFilter(event_time) GROUP BY 1 ORDER BY 1", "short"),
            ("Temperatura del garaje", "timeseries", "SELECT time, room metric, temperature_c value FROM tfm.iot_gold.comfort_hourly WHERE room='garaje' AND $__timeFilter(time) ORDER BY time", "celsius"),
            ("CO₂ del garaje", "timeseries", "SELECT time, room metric, co2_ppm value FROM tfm.iot_gold.air_quality_hourly WHERE room='garaje' AND $__timeFilter(time) ORDER BY time", "ppm"),
        ],
    ),
}


HOME = build_dashboard(
    "tfm-home-overview", "🏠 Home Overview",
    [
        ("Temperatura media actual", "SELECT avg(numeric_state) value FROM tfm.iot_gold.room_latest_state WHERE device_class='temperature'", "celsius"),
        ("Humedad media actual", "SELECT avg(numeric_state) value FROM tfm.iot_gold.room_latest_state WHERE device_class='humidity'", "percent"),
        ("CO₂ medio actual", "SELECT avg(numeric_state) value FROM tfm.iot_gold.room_latest_state WHERE device_class='carbon_dioxide'", "ppm"),
        ("Potencia estimada actual", "SELECT sum(avg_power_kw) value FROM tfm.iot_gold.energy_hourly WHERE time=(SELECT max(time) FROM tfm.iot_gold.energy_hourly)", "kW"),
        ("Confort global", "SELECT avg(comfort_score) value FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time)", "percent"),
        ("Estancias ocupadas", "SELECT count_if(occupied) value FROM tfm.iot_gold.occupancy_hourly WHERE time=(SELECT max(time) FROM tfm.iot_gold.occupancy_hourly)", "short"),
        ("Accesos abiertos", "SELECT count(*) value FROM tfm.iot_gold.room_latest_state WHERE (device_class IN ('door','window','opening') OR entity_id LIKE '%door%' OR entity_id LIKE '%window%') AND lower(state) IN ('on','open','unlocked')", "short"),
        ("Calidad del dato", "SELECT 100*sum(valid_events)/nullif(sum(received_events),0) value FROM tfm.iot_gold.data_quality WHERE $__timeFilter(time)", "percent"),
    ],
    [
        ("Temperatura por estancia", "timeseries", "SELECT time, room metric, temperature_c value FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time) ORDER BY 1,2", "celsius"),
        ("Potencia del hogar", "timeseries", "SELECT time, source metric, sum(avg_power_kw) value FROM tfm.iot_gold.energy_hourly WHERE $__timeFilter(time) GROUP BY 1,2 ORDER BY 1,2", "kW"),
        ("Estado actual por estancia", "table", "SELECT area_id estancia,entity_id,state,numeric_state,unit,device_class,event_time FROM tfm.iot_gold.room_latest_state ORDER BY 1,2", None),
        ("Confort por habitación", "barchart", "SELECT room,avg(comfort_score) comfort_pct FROM tfm.iot_gold.comfort_hourly WHERE $__timeFilter(time) GROUP BY room ORDER BY 2 DESC", "percent"),
    ],
)


QUALITY = build_dashboard(
    "tfm-data-quality", "🔧 IoT & Data Quality",
    [
        ("Sensores activos", "SELECT count_if(activity_status='active') value FROM tfm.iot_gold.sensor_health", "short"),
        ("Sensores sin reportar", "SELECT count_if(activity_status<>'active') value FROM tfm.iot_gold.sensor_health", "short"),
        ("Eventos recibidos 24h", "SELECT sum(received_events) value FROM tfm.iot_gold.data_quality WHERE time >= current_timestamp - INTERVAL '24' HOUR", "short"),
        ("Datos válidos", "SELECT 100*sum(valid_events)/nullif(sum(received_events),0) value FROM tfm.iot_gold.data_quality WHERE $__timeFilter(time)", "percent"),
        ("Datos reales", "SELECT 100*sum(CASE WHEN source<>'synthetic' THEN received_events ELSE 0 END)/nullif(sum(received_events),0) value FROM tfm.iot_gold.data_quality WHERE $__timeFilter(time)", "percent"),
        ("Datos sintéticos", "SELECT 100*sum(CASE WHEN source='synthetic' THEN received_events ELSE 0 END)/nullif(sum(received_events),0) value FROM tfm.iot_gold.data_quality WHERE $__timeFilter(time)", "percent"),
        ("Registros Gold", "SELECT count(*) value FROM tfm.iot_gold.entity_5m WHERE $__timeFilter(window_start)", "short"),
    ],
    [
        ("Frescura del dato", "barchart", "SELECT entity_id, minutes_since_received FROM tfm.iot_gold.sensor_health ORDER BY minutes_since_received DESC NULLS FIRST LIMIT 100", "m"),
        ("Completitud por hora", "heatmap", "SELECT time, source metric, valid_ratio_pct value FROM tfm.iot_gold.data_quality WHERE $__timeFilter(time) ORDER BY 1,2", "percent"),
        ("Real vs sintético", "piechart", "SELECT source, sum(received_events) value FROM tfm.iot_gold.data_quality WHERE $__timeFilter(time) GROUP BY source", "short"),
        ("Calidad por origen", "timeseries", "SELECT time, source metric, valid_ratio_pct value FROM tfm.iot_gold.data_quality WHERE $__timeFilter(time) ORDER BY 1,2", "percent"),
        ("Sensores con problemas", "table", "SELECT source,entity_id,area_id,minutes_since_received,activity_status,last_quality_flag FROM tfm.iot_gold.sensor_health WHERE activity_status<>'active' OR last_quality_flag<>'ok' ORDER BY minutes_since_received DESC NULLS FIRST", None),
    ],
)


INGESTION = build_dashboard(
    "tfm-ingestion", "📥 Ingesta y Calidad por Capas",
    [
        ("Registros Bronze", "SELECT bronze_records value FROM tfm.iot_gold.ingestion_summary", "short"),
        ("Registros Silver", "SELECT silver_records value FROM tfm.iot_gold.ingestion_summary", "short"),
        ("Registros Gold", "SELECT gold_records value FROM tfm.iot_gold.ingestion_summary", "short"),
        ("Registros en cuarentena", "SELECT quarantine_records value FROM tfm.iot_gold.ingestion_summary", "short"),
        ("Pendientes de clasificar", "SELECT greatest(bronze_records-silver_records-quarantine_records,0) value FROM tfm.iot_gold.ingestion_summary", "short"),
        ("Aceptación Silver", "SELECT silver_acceptance_pct value FROM tfm.iot_gold.ingestion_summary", "percent"),
        ("Cobertura Gold", "SELECT gold_coverage_pct value FROM tfm.iot_gold.ingestion_summary", "percent"),
        ("Último registro Bronze", "SELECT last_bronze_record value FROM tfm.iot_gold.ingestion_summary", "dateTimeFromNow"),
        ("Último registro Silver", "SELECT last_silver_record value FROM tfm.iot_gold.ingestion_summary", "dateTimeFromNow"),
        ("Última ventana Gold", "SELECT last_gold_window value FROM tfm.iot_gold.ingestion_summary", "dateTimeFromNow"),
    ],
    [
        ("Volumen actual por capa", "barchart", "SELECT layer,records FROM (SELECT 'Bronze' layer,bronze_records records FROM tfm.iot_gold.ingestion_summary UNION ALL SELECT 'Silver',silver_records FROM tfm.iot_gold.ingestion_summary UNION ALL SELECT 'Gold',gold_records FROM tfm.iot_gold.ingestion_summary UNION ALL SELECT 'Cuarentena',quarantine_records FROM tfm.iot_gold.ingestion_summary) ORDER BY records DESC", "short"),
        ("Registros procesados por hora y capa", "timeseries", "SELECT time,layer metric,records value FROM tfm.iot_gold.ingestion_hourly WHERE $__timeFilter(time) ORDER BY 1,2", "short"),
        ("Aceptación por origen", "barchart", "SELECT source,acceptance_pct FROM tfm.iot_gold.ingestion_quality ORDER BY acceptance_pct", "percent"),
        ("Volumen y calidad por origen", "table", "SELECT source,bronze_records,accepted_records,quarantined_records,duplicate_records,acceptance_pct,last_processed_at,last_event_at,avg_source_latency_ms FROM tfm.iot_gold.ingestion_quality ORDER BY bronze_records DESC", None),
        ("Causas de cuarentena", "barchart", "SELECT quality_flag,source,records FROM tfm.iot_gold.quarantine_causes ORDER BY records DESC", "short"),
        ("Frescura por sensor", "barchart", "SELECT entity_id,source,minutes_since_received FROM tfm.iot_gold.sensor_health ORDER BY minutes_since_received DESC NULLS FIRST LIMIT 100", "m"),
        ("Último registro por origen", "table", "SELECT source,last_processed_at,last_event_at,bronze_records,acceptance_pct FROM tfm.iot_gold.ingestion_quality ORDER BY last_processed_at DESC", None),
        ("Calidad temporal", "timeseries", "SELECT time,source metric,valid_ratio_pct value FROM tfm.iot_gold.data_quality WHERE $__timeFilter(time) ORDER BY 1,2", "percent"),
    ],
)


def main() -> None:
    MAIN.mkdir(parents=True, exist_ok=True)
    TECHNICAL.mkdir(parents=True, exist_ok=True)
    for filename in [
        "02-energy-cost.json", "02-environment.json", "03-activity.json",
        "05-device-status.json", "06-ingestion-control.json",
    ]:
        (MAIN / filename).unlink(missing_ok=True)
    for filename in ["01-overview.json", "04-quality.json"]:
        (TECHNICAL / filename).unlink(missing_ok=True)
    for filename, dashboard in DASHBOARDS.items():
        (MAIN / filename).write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (MAIN / "01-home-overview.json").write_text(
        json.dumps(HOME, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (TECHNICAL / "10-data-quality.json").write_text(
        json.dumps(QUALITY, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (TECHNICAL / "11-ingestion.json").write_text(
        json.dumps(INGESTION, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
