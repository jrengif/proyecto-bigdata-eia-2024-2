import pymysql
import boto3
import json
import csv
from io import StringIO
import traceback
 

# Función para conectar a la base de datos RDS
def connect_to_rds(event):
    print("Configurando conexion a RDS")

    # Se obtienen las credenciales de acceso a RDS
    rds_host = event['RDS_HOST']
    rds_user = event['RDS_USER']
    rds_password = event['RDS_PASSWORD']
    rds_db = event['RDS_DB']
    rds_port = event['RDS_PORT']
    
    try:
        conn = pymysql.connect(
            host=rds_host,
            user=rds_user,
            password=rds_password,
            db=rds_db,
            port=rds_port
        )
        return conn
    except pymysql.MySQLError as e:
        print(f"Error al conectar a la base de datos RDS: {e}")
        traceback.print_exc()
        return None
 
# Función para consultar datos desde la base de datos RDS
def query_rds(conn, event):
    print("Realizando query en RDS")
    try:
        table_name = event["TABLE_NAME"]
        query = f"SELECT * FROM {table_name}"
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
        return result
    except Exception as e:
        print(f"Error al consultar la BD RDS: {e}")
        traceback.print_exc()
        return None
 
# Función para subir datos a S3
def upload_to_s3(data, event):
    print("Subiendo datos en S3")
    try:
        
        # Se obtiene rutas de S3 donde se cargarán los datos obtenidos desde MySQL
        bucket = event["BUCKET_NAME_CURATED"]
        key = event["FILE_KEY_CURATED_SQL"]
        
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)
        
        # Se escriben los encabezados de la tabla en el CSV
        csv_writer.writerow(["teams", "seasons", "players", "matches", "goals", "assists", "seasons_ratings"])  
        
        # Se recorren uno por uno los registros obtenidos en la consulta para escribirlos en el buffer del csv
        for row in data:
            csv_writer.writerow([row[0], row[1], row[2], row[3], row[4], row[5], row[6]])
            
        # Se hace la conexion a S3 para almacenar el archivo
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_buffer.getvalue()
        )
        
    except Exception as e:
        print(f"Error al subir datos en S3: {e}")
        traceback.print_exc()
        return None
 
 
def lambda_handler(event, context):    
    
    conn = connect_to_rds(event)
    if not conn:
        return {
            "statusCode": 500,
            "body": json.dumps("Error al conectar a la base de datos RDS.")
        }
    try:
        data = query_rds(conn,event)
        if not data:
            return {
                "statusCode": 404,
                "body": json.dumps("No se encontraron datos en la base de datos RDS.")
            }
 
        upload_to_s3(data, event)
        return {
            "statusCode": 200,
            "body": json.dumps("Datos transferidos exitosamente a S3.")
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error al procesar datos: {e}")
        }
    finally:
        conn.close()