import json
import boto3
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
from io import StringIO
import os

# 기본 DAG 인자 설정
default_args = {
    'owner': 'seungjun',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
@dag(
    dag_id='high_volatility_kr_stock',
    default_args=default_args,
    description='Fetch and process exchange rate data for beginner stock investors',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def high_volatility_kr_stock():

    @task(task_id="read_csv_from_s3")
    def read_csv_from_s3():
        """S3에서 CSV 파일을 읽어 상위 3개의 회사 코드를 반환합니다."""
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        s3_key = 'kr_stock_data/kr_top100.csv'
        csv_content = s3_hook.read_key(s3_key, s3_bucket)

        df = pd.read_csv(StringIO(csv_content), encoding='utf-8')
        logging.info(f"DataFrame columns: {df.columns.tolist()}")  # 열 이름 출력
        top_100_codes = df['CompanyCode'].head(3).tolist()
        return top_100_codes
    
    @task(task_id="fetch_csv_files")
    def fetch_csv_files(top_100_codes):
        """상위 3개 회사 코드에 대한 CSV 파일을 S3에서 읽어 로컬에 저장합니다."""
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        local_files = []

        for code in top_100_codes:
            # 코드 번호를 6자리로 포맷팅
            formatted_code = f'{int(code):06d}'
            file_key = f'kr_stock_data/stock_data/{formatted_code}.csv'
            try:
                csv_content = s3_hook.read_key(file_key, s3_bucket)
                temp_csv = f"/tmp/{formatted_code}.csv"
                
                # 로컬 임시 파일에 저장
                with open(temp_csv, 'w') as f:
                    f.write(csv_content)
                
                local_files.append(temp_csv)
                logging.info(f"Successfully processed {file_key}")
            except Exception as e:
                logging.error(f"Error reading file {file_key} from S3: {e}")

        return local_files
    
    @task(task_id="invoke_lambda_for_volatility")
    def invoke_lambda_for_volatility(local_files):
        """
        Lambda 함수를 호출하여 변동성 높은 데이터를 필터링하고 S3에 저장합니다.
        """
        lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
        lambda_function_name = 'newbstock_high_volatillity_kr_stock'
        
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

                # Lambda의 반환값 처리
                response_payload = json.loads(response['Payload'].read())
                if response_payload.get('statusCode') == 200:
                    body = json.loads(response_payload['body'])
                    if 'processed_key' in body:
                        processed_files.append(body['processed_key'])
                    else:
                        logging.warning(f"Lambda did not return a processed_key for {s3_key}")
                else:
                    logging.error(f"Lambda 호출 중 오류 발생: {response_payload.get('body')}")

            except Exception as e:
                logging.error(f"Lambda 호출 중 오류 발생: {e}")

            # 로컬 파일 삭제
            os.remove(file_path)

        return processed_files

    @task(task_id="truncate_table")
    def truncate_table():
        """
        Redshift 테이블에서 데이터를 삭제합니다.
        """
        redshift_conn_id = 'redshift_conn'
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
        
        truncate_sql = "TRUNCATE TABLE public.high_volatility_kr;"
        
        try:
            redshift_hook.run(truncate_sql)
            logging.info("테이블을 성공적으로 비웠습니다.")
        except Exception as e:
            logging.error(f"테이블을 비우는 중 오류 발생: {e}")

    @task(task_id="load_to_redshift")
    def load_to_redshift(processed_files):
        """
        S3에서 Lambda를 통해 처리된 파일들을 Redshift로 로드합니다.
        """
        redshift_conn_id = 'redshift_conn'
        aws_conn_id = 'aws_default'
        redshift_table = 'public.high_volatility_kr'
        s3_bucket = 'team-won-2-bucket'
        
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        
        for s3_key in processed_files:
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
    truncate_table() >> load_to_redshift(processed_files)

# DAG 인스턴스 생성
dag = high_volatility_kr_stock()
