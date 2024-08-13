import csv
import requests
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import logging
from io import StringIO

# 기본 DAG 인자 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
@dag(
    dag_id='high_volatility_us_stock',
    default_args=default_args,
    description='Fetch and process exchange rate data for beginner stock investors',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def high_volatility_us_stock():

    @task(task_id="read_csv_from_s3")
    def read_csv_from_s3():
        """S3에서 CSV 파일을 읽어 상위 3개의 주식 코드를 반환합니다."""
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        s3_key = 'us_stock_data/us_stock_top100.csv'
        csv_content = s3_hook.read_key(s3_key, s3_bucket)

        df = pd.read_csv(StringIO(csv_content))
        top_100_codes = df['code'].head(3).tolist()  # 상위 3개 주식 코드 추출
        return top_100_codes
    
    @task(task_id="fetch_csv_files")
    def fetch_csv_files(top_100_codes):
        """상위 3개 주식 코드에 대한 CSV 파일을 S3에서 읽어 로컬에 저장합니다."""
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        local_files = []

        for code in top_100_codes:
            file_key = f'us_stock_data/history/{code}.csv'
            try:
                csv_content = s3_hook.read_key(file_key, s3_bucket)
                df = pd.read_csv(StringIO(csv_content))
                
                # 임시 파일에 CSV 내용을 저장
                temp_csv = f"/tmp/{code}.csv"
                df.to_csv(temp_csv, index=False)
                
                local_files.append(temp_csv)
                logging.info(f"Successfully processed {file_key}")
            except Exception as e:
                logging.error(f"Error reading file {file_key} from S3: {e}")

        return local_files
    
    @task(task_id="find_high_volatility_days")
    def find_high_volatility_days(local_files):
        """로컬에 저장된 주식 데이터에서 변동성이 큰 날을 찾아 S3에 업로드합니다."""
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        
        for file_path in local_files:
            df = pd.read_csv(file_path)
            if not df.empty:
                code = df['code'].iloc[0]  # 주식 코드 추출
                
                # 변동률 계산
                df['Close'] = df['Close'].astype(float)
                df['Prev_Close'] = df['Close'].shift(1)
                df['Change'] = (df['Close'] - df['Prev_Close']) / df['Prev_Close'] * 100

                # 변동률이 10% 이상인 날짜 찾기
                high_volatility = df[df['Change'].abs() > 10]
                if not high_volatility.empty:
                    high_volatility_file = f"/tmp/{code}_high_volatility.csv"
                    high_volatility.to_csv(high_volatility_file, index=False)
                    
                    # S3에 업로드
                    s3_key = f'newb_data/stock_data/us/{code}_high_volatility.csv'
                    s3_hook.load_file(
                        filename=high_volatility_file,
                        key=s3_key,
                        bucket_name=s3_bucket,
                        replace=True
                    )
                    logging.info(f"Successfully uploaded {s3_key} to S3")
                else:
                    logging.info(f"No high volatility days found for {code}")
            else:
                logging.warning(f"The file {file_path} is empty and will be skipped")

    # DAG 실행 순서 정의
    top_100_codes = read_csv_from_s3()
    local_files = fetch_csv_files(top_100_codes)
    find_high_volatility_days(local_files)

# DAG 인스턴스 생성
dag = high_volatility_us_stock()
