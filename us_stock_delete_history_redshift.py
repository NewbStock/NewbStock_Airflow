from datetime import datetime, timedelta, date
import logging
import boto3
import time
import FinanceDataReader as fdr
import io
import psycopg2
import pandas as pd

# Redshift 연결
conn = psycopg2.connect(
    host="team-won-2-redshift-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com",
    database="dev",
    user="newbstock_admin",
    password="Newbstock001!",
    port=5439,
)
conn.autocommit = True
cur = conn.cursor()

bucket_name = "team-won-2-bucket"
prefix = "us_stock_data/history/"

s3_client = boto3.client('s3')
obj_list = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
contents_list = obj_list['Contents']
file_list = [content['Key'].replace(prefix, '') for content in contents_list]

data = pd.read_csv(
    io.BytesIO(
        s3_client.get_object(
            Bucket=bucket_name,
            Key="us_stock_data/us_stock_top100.csv"
        )['Body'].read()
    )
)

company_name_list = list(data['name'])
company_code_list = list(data['code'])

for file in file_list:
    code = file.replace(".csv", '')
    if code not in company_code_list:
        response = s3_client.delete_object(
            Bucket=bucket_name,
            Key=prefix + file
        )
        print(response)
        
        delete_sql = f"DELETE FROM stock_history.us_stock_data WHERE code = '{code}';"
        cur.execute(delete_sql)
    
cur.close()
conn.close()
