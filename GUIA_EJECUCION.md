# FleetLogix — Guía de ejecución y seguridad para evaluación

## Acceso a los datos
El proyecto utiliza PostgreSQL local y Snowflake. Por seguridad no se incluyen contraseñas, tokens PAT ni claves AWS. Los resultados guardados en los notebooks permiten revisar las evidencias históricas sin ejecutar conexiones. **La presencia de resultados no demuestra que las conexiones sigan activas.**

## Preparación
1. Instalar Python 3.12 y dependencias de los notebooks y scripts (psycopg2-binary, pandas, numpy, faker, snowflake-connector-python, holidays, schedule, python-dotenv, etc., según el componente).
2. Disponer de una base PostgreSQL `fleetlogix` con las tablas de origen y datos del proyecto. `localhost` solo funciona si la base está en la misma máquina.
3. Disponer de una cuenta Snowflake propia con permisos para el warehouse `FLEETLOGIX_WH`, database `FLEETLOGIX_DW`, schema `ANALYTICS` y role `FLEETLOGIX_ETL_ROLE`, o adaptar los nombres en el script.
4. Configurar las variables del archivo `CONFIGURACION_EJEMPLO.env.txt` en el entorno privado de ejecución, o proporcionar las credenciales por las solicitudes interactivas. Nunca subir el archivo `.env` con valores reales.
5. Ejecutar notebooks en orden 1, 2, 3 y 4 según sus instrucciones. Los scripts ETL no deben ejecutarse simultáneamente en dos procesos. El Avance 4 es una propuesta conceptual, no un despliegue.

## Autenticación
- `Scripts/01_data_generation.py`: solicita contraseña PostgreSQL cuando no existe `FLEETLOGIX_PASSWORD`.
- `Notebooks/Avance_1_DataGeneration.ipynb` y `Avance_2_SQLAnalysis.ipynb`: usan variables de entorno o `getpass`.
- `Scripts/05_etl_pipeline.py`: PostgreSQL (`PGPASSWORD` o `getpass`) y Snowflake (`SF_ACCOUNT`, `SF_USER`, `SF_TOKEN` o solicitudes interactivas). El PAT debe ser válido y tener permisos suficientes.
- `Scripts/aws_setup.py`: no desplegar como parte de la evaluación; requiere configuración AWS y contraseña RDS privada, además de revisión de seguridad.

## Límites de reproducibilidad
Los archivos no incluyen las 500 mil filas del origen ni credenciales. El evaluador puede revisar SQL, código y evidencias, pero no reproducir el ETL de punta a punta sin provisionar datos y servicios. Las salidas de Jupyter son capturas de ejecuciones previas. El diccionario de datos es referencial y debe contrastarse con el catálogo de la base real.

## Seguridad antes de compartir
No adjuntar `.env`, logs, archivos con tokens ni capturas de configuración sensible. Si alguna credencial se compartió antes en otro archivo, revocarla o rotarla en el proveedor; eliminarla de un ZIP no invalida una credencial expuesta.
