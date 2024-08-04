import json
import pymysql
import boto3
import csv
from io import StringIO



# Lambda handler function
def lambda_handler(event, context):
    # Get bucket and file information from the event
    
    print("leyendo datos evento")
    # RDS settings
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
    
    print("Iniciando configuracion conexion")

    cursor = conn.cursor()
    
    print("conectado")

    
    s3_client = boto3.client('s3')
    # Download CSV file from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    csv_content = response['Body'].read().decode('utf-8')
    
    # Parse CSV
    csv_reader = csv.reader(StringIO(csv_content))
    
    # Skip the header row
    next(csv_reader)
    
    for row in csv_reader:
        print(row)
        # Adjust this query according to your table schema
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
