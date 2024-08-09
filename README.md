# Proyecto Final Big Data e Integración de datos masivos
 Universidad EIA

Integrantes:
- Juan Camilo Pareja Osorio
- José Orlando Rengifo Caicedo

# Descripción

Este repositorio contiene el codigo fuente y los diagramas de arquitectura del proyecto final de la materia Big Data e Integración de datos masivos. En este se busca crear un pipeline de datos usando algunas tecnologias de BigData.

# Estructura del repositorio

```plaintext
─proyecto-bigdata-eia-2024-2
 ├───arquitectura
 ├───colab_notebooks
 │   └─download_data_snowflake_to_s3.ipynb
 ├───lambda_code
 │   ├───download_data_mysql_to_s3
 │   └───upload_data_to_mysql
 ├───sql_scripts
 │   └───mysql_bd_mysql_proyecto_eia.sql
 └───parametros_accesos.json

```


- **Arquitectura:** Contiene el diagrama de flujo de datos de la solución ademas de los componentes y servicios utilizados.
- **colab_notebooks:** Contiene codigos de notebooks en colab
  - **download_data_snowflake_to_s3.ipynb** Notebook que se encarga de hacer un query sobre Snoflake e insertar el resultado de esa consulta en formato .csv en S3.
- **lambda_code:** Contiene el codigo de las lambdas creadas para el proyecto.
  - **download_data_mysql_to_s3:** Se encarga de consultar los datos en MySQL RDS y posteriormente insertarlos en formato .csv en S3.
  - **upload_data_to_mysql:** Se encarga de tomar los datos en S3 y realizar un INSERT en la tabla de Mysql RDS.
- **sql_scripts:** scripts de sql
  - **mysql_bd_mysql_proyecto_eia.sql:** Contiene las sentencias de SQL para Crear la base de datos dentro del motor de SQL ademas de la tabla, fueron ejecutados desde MySQL Workbench.
- **parametros_accesos.json:** Contiene las credenciales de acceso de los servicios de MySQL RDS, Snowflake, rutas de acceso a S3, credenciales de AWS.


# Componentes Arquitectura
![Componentes Arquitectura](arquitectura\componentes_arquitectura.png)

# Arquitectura Propuesta

A continuación se pueden apreciar las 4 capas de servicios usados en la solución:

- **Almacenamiento:** Conformado por dos buckets de S3 que representan la zona de datos crudos (RAW) y la zona de datos curados (CURATED) respectivamente, ademas de un motor SQL  

![Arquitectura Proyecto final](arquitectura\diagrama_arquitectura.png)

