# Modelo ambiental confirmado

Nombres visibles: Habitación principal, Habitación 2, Habitación 3, Baño 1, Baño 2, Baño 3, Salón y Hall.
Baño 1 corresponde a Habitación principal; Baño 2 a Habitación 2; Baño 3 a Habitación 3. Los baños son estancias distintas de sus habitaciones asociadas.

Los dispositivos ambientales físicos proceden de la integración Matter sobre Zigbee de Home Assistant. Cada dispositivo puede exponer temperatura y humedad; el inventario local es la fuente de verdad de las entidades disponibles.

Las vistas reference.rooms y reference.environment_entities registran las relaciones confirmadas. Los identificadores analytical_device_id son claves analíticas estables, no identificadores de hardware.

silver.environment_events ofrece eventos ambientales reales con claves analíticas y nombres neutros. Su granularidad sigue siendo un evento por entidad, no una lectura conjunta de temperatura y humedad. Los sintéticos siguen disponibles en la Silver general y requieren su correspondencia separada antes de incorporarlos a este modelo.

Se conserva source_entity_id para trazabilidad. No se modifica entity_id ni event_id en el histórico, porque participan en la deduplicación. Grafana deberá utilizar room_name y analytical_entity_id; la actualización de dashboards está pendiente. Esta capa no anonimiza el payload original ni los identificadores de origen y no debe publicarse sin revisar esos campos.

Otros elementos confirmados para modelar después: cerradura y apertura de puerta en Hall; tres persianas en Salón; dos relés de una misma lámpara del Salón (encendido y regulación). No se han inferido relaciones del resto de dispositivos.

Las vistas se provisionan mediante trino/iceberg-views.sql al arrancar trino-iceberg-init.
