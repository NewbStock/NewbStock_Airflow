from datetime import datetime, timedelta
import logging
import os
import json
import pandas as pd
from io import StringIO

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import boto3

# 주말 여부를 확인하는 함수
def is_weekend():
    today = datetime.today().weekday()
    # 5 = Saturday, 6 = Sunday
    return today in [5, 6]

# 기본 DAG 설정
default_args = {
    'owner': 'seungjun',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='high_volatility_us_stock',
    default_args=default_args,
    description='초보 투자자를 위한 변동성 높은 미국 주식 데이터 수집 및 처리',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['stocks'],
)
def high_volatility_us_stock():
    """
    초보 투자자를 위한 변동성 높은 미국 주식 데이터를 수집 및 처리하는 DAG입니다.
    """

    if is_weekend():
        logging.info("주말이므로 DAG 실행을 건너뜁니다.")
        return

    @task(task_id="read_csv_from_s3")
    def read_csv_from_s3():
        """
        S3 버킷에서 상위 100개 미국 주식 코드를 포함한 CSV 파일을 읽어옵니다.
        """
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        s3_key = 'us_stock_data/us_stock_top100.csv'
        csv_content = s3_hook.read_key(s3_key, s3_bucket)

        df = pd.read_csv(StringIO(csv_content))
        top_100_codes = df['code'].head(3).tolist()
        return top_100_codes
    
    @task(task_id="fetch_csv_files")
    def fetch_csv_files(top_100_codes):
        """
        S3에서 상위 3개의 미국 주식 코드에 대한 역사 데이터를 가져옵니다.
        """
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        local_files = []

        for code in top_100_codes:
            file_key = f'us_stock_data/history/{code}.csv'
            try:
                csv_content = s3_hook.read_key(file_key, s3_bucket)
                df = pd.read_csv(StringIO(csv_content))
                
                temp_csv = f"/tmp/{code}.csv"
                df.to_csv(temp_csv, index=False)
                
                local_files.append(temp_csv)
                logging.info(f"{file_key} 파일을 성공적으로 처리했습니다.")
            except Exception as e:
                logging.error(f"S3에서 {file_key} 파일을 읽는 중 오류 발생: {e}")

        return local_files

    @task(task_id="invoke_lambda_for_volatility")
    def invoke_lambda_for_volatility(local_files):
        """
        Lambda 함수를 호출하여 변동성 높은 데이터를 필터링하고 S3에 저장합니다.
        """
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        lambda_function_name = 'newbstock_high_volatility_us_stock'
        
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'

        processed_files = []

        for file_path in local_files:
            # S3에 파일 업로드
            s3_key = f"temp/{file_path.split('/')[-1]}"
            s3_hook.load_file(filename=file_path, key=s3_key, bucket_name=s3_bucket, replace=True)

            # Lambda에 전달할 페이로드에 S3 경로 포함
            payload = {
                's3_bucket': s3_bucket,
                's3_key': s3_key,
                'processed_bucket': s3_bucket
            }

            try:
                response = lambda_client.invoke(
                    FunctionName=lambda_function_name,
                    InvocationType='RequestResponse',  # 동기 호출
                    Payload=json.dumps(payload)
                )
            except Exception as e:
                logging.error(f"Lambda 호출 중 오류 발생: {e}")

            # 로컬 파일 삭제
            os.remove(file_path)

        return processed_files

    @task(task_id="load_to_redshift")
    def load_to_redshift(processed_files):
        """
        S3에서 Lambda를 통해 처리된 파일들을 Redshift로 로드합니다.
        주말에는 데이터를 로드하지 않습니다.
        """
        redshift_conn_id = 'redshift_conn'
        aws_conn_id = 's3_conn'
        redshift_table = 'public.high_volatility_us'
        s3_bucket = 'team-won-2-bucket'
        
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        
        today = datetime.today().strftime('%Y-%m-%d')

        for s3_key in processed_files:
            if today not in s3_key:
                logging.info(f"{s3_key}은 오늘({today})의 데이터가 아니므로 건너뜁니다.")
                continue

            # Redshift COPY 명령어 실행
            copy_sql = f"""
                COPY {redshift_table}
                FROM 's3://{s3_bucket}/{s3_key}'
                ACCESS_KEY_ID '{s3_hook.get_credentials().access_key}'
                SECRET_ACCESS_KEY '{s3_hook.get_credentials().secret_key}'
                CSV
                IGNOREHEADER 1
                DELIMITER ','
                REGION 'ap-northeast-2';
            """
            try:
                redshift_hook.run(copy_sql)
                logging.info(f"{s3_key} 데이터를 Redshift로 성공적으로 로드했습니다.")
            except Exception as e:
                logging.error(f"Redshift로 데이터를 로드하는 중 오류 발생: {e}")

    # DAG의 태스크들 연결
    top_100_codes = read_csv_from_s3()
    local_files = fetch_csv_files(top_100_codes)
    processed_files = invoke_lambda_for_volatility(local_files)
    load_to_redshift(processed_files)

# DAG 인스턴스 생성
dag = high_volatility_us_stock()
