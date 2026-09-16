# Gold ambiental diario

Vista Trino: `tfm.gold.environment_daily`.

La vista resume los eventos ambientales reales por estancia y día. Usa las entidades de temperatura y humedad de los dispositivos Matter integrados en Home Assistant, excluye cuarentena y conserva los nombres neutros del modelo.

Incluye medias, mínimos y máximos interiores, número de eventos, número de entidades recibidas, primera y última recepción, indicadores de cobertura de temperatura/humedad y el contexto diario de la estación AEMET 9263D (Pamplona, Aeropuerto). Cuando coinciden las fechas, calcula también la diferencia entre temperatura interior y exterior. Si no existe dato meteorológico para un día, las columnas exteriores quedan en `NULL`; no se imputa ni se trata como cero.

La granularidad diaria es deliberada: AEMET se ingiere como climatología diaria. Para análisis horario de calefacción se necesitaría una fuente meteorológica horaria posterior.

La Gold es actualmente una vista SQL sobre Iceberg, por lo que se recalcula al consultar y no introduce una segunda copia física. La siguiente iteración puede materializarla con dbt si los dashboards necesitan rendimiento o snapshots.
