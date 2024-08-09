import json
import pymysql
import boto3
import csv
from io import StringIO



# Lambda handler function
def lambda_handler(event, context):
    
    # Se obtienen las credenciales de RDS
    rds_host = event['RDS_HOST']
    rds_user = event['RDS_USER']
    rds_password = event['RDS_PASSWORD']
    rds_db = event['RDS_DB']
    bucket_name = event['BUCEKT_NAME']
    file_name = event['FILE_KEY']
    table_name = event['TABLE_NAME']
    print("finalizando datos evento")
   
    # Connect to the RDS MySQL instance
    conn = pymysql.connect(
        host=rds_host,
        user=rds_user,
        password=rds_password,
        db=rds_db,
        connect_timeout=5
    )
    cursor = conn.cursor()
    
    # Se obtiene el archivo csv de la zona raw en S3
    s3_client = boto3.client('s3')    
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    csv_content = response['Body'].read().decode('utf-8')
    
    # Objeto para hacer la lectura mas facil del csv
    csv_reader = csv.reader(StringIO(csv_content))
    
    # Nos saltamos la primera fila del csv que es la que contiene el encabezado
    next(csv_reader)
    
    # Se recorren los datos fila por fila para hacer la insercion en MySQL registros por registro
    for row in csv_reader:
        
        query = f"INSERT INTO {table_name} (teams, seasons, players, matches, goals, assists, seasons_ratings) VALUES (%s, %s, %s, %s, %s, %s, %s);"
        cursor.execute(query, (row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
    
    # Commit the transaction
    conn.commit()

    # Close the connection
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps('CSV file processed and uploaded to RDS MySQL')
    }
