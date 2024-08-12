# airflow library
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta, date
import logging
import boto3
import time
import FinanceDataReader as fdr
import io
import psycopg2
import pandas as pd

def stock_history_upload():
    ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
    SECRET_KEY = Variable.get("AWS_SECRET_KEY")
    bucket_name = "team-won-2-bucket"
    prefix = "us_stock_data/history/"

    s3_client = boto3.client(
        service_name='s3',
        region_name="ap-northeast-2",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    # 현재 저장된 파일 리스트 추출
    obj_list = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    contents_list = obj_list['Contents']
    file_list = []
    for content in contents_list:
        file_name = content['Key'].replace(prefix, '')
        file_list.append(file_name)
    # 현재 top100 리스트 추출
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

    for i in range(len(company_code_list)):
        company_name = company_name_list[i] # 회사 이름
        company_code = company_code_list[i] # 회사 코드(심벌)

        file_name = company_code + ".csv"
        save_file_name = prefix + file_name

        read_file = pd.read_csv(
            io.StringIO(
                s3_client.get_object(
                    Bucket=bucket_name,
                    Key=save_file_name
                )['Body'].read()
            )
        )   

        try:
            if file_name in file_list:
                yesterday = date.today() - timedelta(days=1)
                update_hist = fdr.DataReader('YAHOO:' + company_code, yesterday)
                update_hist.insert(0, 'Code', company_code)
                update_hist.insert(0, 'Name', company_name)
                update_hist.rename(columns={'Open':'OpenValue'}, inplace=True)
                hist = pd.concat([read_file, update_hist])
            else:        
                hist = fdr.DataReader('YAHOO:' + company_code)
                hist.insert(0, 'Code', company_code)
                hist.insert(0, 'Name', company_name)
                hist.rename(columns={'Open':'OpenValue'}, inplace=True)
            
            s3_client.put_object(
                Body=hist.to_csv(index=True).encode(),
                Bucket=bucket_name,
                Key=save_file_name
            )
            logging.info(company_name + " file s3 upload done")
            
        except Exception as ex: 
            print('data error', ex)

default_args = {
    'owner': 'joonghyeon',
    'depends_in_past': False,
    'start_date': datetime(2024, 7, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}
with DAG(
    dag_id='us_stock_etl',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    schedule='0 11 * * 2-6', # 적당히 조절

) as dag:
    history_upload_to_s3_task = PythonOperator (
        task_id='history_upload_to_s3',
        python_callable=stock_history_upload,
        dag=dag
    )

    upload_to_redshift_task = GlueJobOperator(
        task_id='upload_to_redshift',
        job_name='newbstock_kr_stock_data_s3_to_redshift',
        script_location='s3://team-won-2-glue-bucket/scripts/newbstock_us_stock_data_s3_to_redshift.py',  # Glue Job에 필요한 스크립트 경로
        region_name='ap-northeast-2',
        iam_role_name='newbstock_glue_role',
        dag=dag
    )

    history_upload_to_s3_task >> upload_to_redshift_task