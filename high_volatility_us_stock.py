import json
import boto3
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
import pandas as pd
import logging
from io import StringIO
import os

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
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def high_volatility_us_stock():
    """
    초보 투자자를 위한 변동성 높은 미국 주식 데이터를 수집 및 처리하는 DAG입니다.
    """

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
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        lambda_function_name = 'newbstock_high_volatility_us_stock'
        
        # Airflow Connections에서 Redshift 연결 정보 가져오기
        redshift_conn = BaseHook.get_connection('redshift_conn')
        redshift_config = {
            'host': redshift_conn.host,
            'port': redshift_conn.port,
            'dbname': redshift_conn.schema,
            'user': redshift_conn.login,
            'password': redshift_conn.password,
            'table': 'high_volatility_days'
        }

        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'

        for file_path in local_files:
            # S3에 파일 업로드
            s3_key = f"temp/{file_path.split('/')[-1]}"
            s3_hook.load_file(filename=file_path, key=s3_key, bucket_name=s3_bucket, replace=True)

            # Lambda에 전달할 페이로드에 S3 경로 포함
            payload = {
                's3_bucket': s3_bucket,
                's3_key': s3_key,
                'redshift_config': redshift_config
            }

            try:
                response = lambda_client.invoke(
                    FunctionName=lambda_function_name,
                    InvocationType='Event',  # 또는 'RequestResponse'로 변경하여 동기 호출 가능
                    Payload=json.dumps(payload)
                )
                logging.info(f"Invoked Lambda function {lambda_function_name} with response: {response}")
            except Exception as e:
                logging.error(f"Lambda 호출 중 오류 발생: {e}")

            # 로컬 파일 삭제
            try:
                os.remove(file_path)
                logging.info(f"로컬 파일 {file_path} 삭제 완료")
            except Exception as e:
                logging.error(f"로컬 파일 {file_path} 삭제 중 오류 발생: {e}")


    # DAG의 태스크들 연결
    top_100_codes = read_csv_from_s3()
    local_files = fetch_csv_files(top_100_codes)
    invoke_lambda_for_volatility(local_files)

# DAG 인스턴스 생성
dag = high_volatility_us_stock()
