# Integración Sonoff

Home Assistant publica los cambios de todas las entidades de la integración
Sonoff mediante la automatización `TFM - Sonoff - publicar eventos en MQTT`.

## Contrato

- Origen: `source=sonoff`.
- Tópico: `tfm/sonoff/events/<dominio>/<entity_id>`.
- El payload contiene `entity_id`, `domain`, `state`, `attributes` y las marcas
  de tiempo del evento, actualización, ingesta y puente MQTT.
- El ingestor se suscribe a `tfm/+/events/#`, por lo que Sonoff comparte el
  mismo flujo de Bronze, Silver y Gold que el resto de orígenes.

## Alcance actual

La automatización cubre los 13 dispositivos Sonoff registrados en Home
Assistant y todas sus entidades, incluidos sensores de conectividad, energía,
persianas, luces y relés. Los nombres personales se mantienen fuera de las
capas analíticas; cualquier renombrado para Grafana se realiza mediante el
modelo de referencia de Trino.

La tabla física de eventos conserva el nombre histórico
`lake.iot_bronze.matter_events`/`lake.iot_silver.matter_events`, pero su columna
`source` identifica correctamente cada origen, incluido `sonoff`.
