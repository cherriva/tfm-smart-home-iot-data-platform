# Ingesta batch Datadis y Airflow

La canalización `datadis_daily_consumption` descarga la curva horaria de los suministros que el usuario tiene autorizados en Datadis. Se ejecuta todos los días a las 11:30 (Europe/Madrid) y puede relanzarse: tanto los ficheros como la carga Iceberg son idempotentes.

## Configuración

Configurar `.env` a partir de `.env.example`. Las únicas credenciales son `DATADIS_USERNAME` y `DATADIS_PASSWORD`; no se incluyen en el repositorio, logs ni imagen Docker. `DATADIS_LOOKBACK_MONTHS` controla la ventana móvil que se vuelve a consultar (2 meses por defecto), una medida para incorporar lecturas corregidas por la distribuidora.
`DATADIS_REQUEST_DELAY_SECONDS` separa las consultas de varios suministros (65 segundos por defecto) para respetar el límite de frecuencia de Datadis.

Para desarrollo local, `DATADIS_S3_ENDPOINT` es `http://localhost:9000`; el contenedor Airflow usa automáticamente `http://minio:9000`.

## Flujo

1. Autentica contra Datadis y consulta los suministros disponibles.
2. Para cada suministro pide la curva horaria de la ventana configurada.
3. Conserva la respuesta sin transformar en `s3://tfm-bronze/datadis/consumption/...`. La ruta no expone el CUPS: emplea un identificador SHA-256 truncado. Antes de continuar, verifica el checksum del objeto almacenado.
4. Convierte las lecturas a `tfm.iot_silver.datadis_consumption_hourly`, tipando fecha, periodo, consumo de red, excedente, generación y autoconsumo. Un `MERGE` por `cups`, fecha y periodo evita duplicados y actualiza correcciones.
5. dbt construye `tfm.gold.grid_energy_daily`, con consumo diario de red, excedentes, generación, autoconsumo, número de lecturas y suministros.

El dashboard **02 - Energy & Cost** usa esta última vista. El proyecto aún no incorpora una tarifa eléctrica, por lo que presenta energía (kWh), no una estimación de euros.

## Ejecución y recuperación

En Airflow, lanzar `datadis_daily_consumption` desde la interfaz o con una configuración opcional como `{"as_of":"2026-09-14"}`. Esta fecha solo determina el final de la ventana; no altera la fuente. Si falla una petición o la validación, el DAG falla y se reintenta. La copia Bronze que ya se hubiese guardado se conserva, y el siguiente intento converge mediante el mismo `MERGE`.

La integración usa la API de Datadis con los permisos concedidos a la cuenta; la disponibilidad y correcciones de lecturas dependen de la distribuidora. Consultar la documentación y condiciones aplicables en [Datadis](https://datadis.es/).
