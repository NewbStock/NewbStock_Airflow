import csv
import requests
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import logging
from io import StringIO

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='stock_top3',
    default_args=default_args,
    description='Fetch and process exchange rate data for beginner stock investors',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def stock_top3():

    @task(task_id="read_csv_from_s3")
    def read_csv_from_s3():
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        s3_key = 'us_stock_data/us_stock_top100.csv'
        csv_content = s3_hook.read_key(s3_key, s3_bucket)

        df = pd.read_csv(StringIO(csv_content))
        top_3_codes = df['Code'].head(3).tolist()
        return top_3_codes
    
    @task(task_id="fetch_csv_files")
    def fetch_csv_files(top_3_codes):
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        local_files = []

        for code in top_3_codes:
            file_key = f'us_stock_data/history/{code}.csv'
            s3_key = f'newb_data/stock_data/{code}.csv'
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
        high_volatility_days = []
        
        for file_path in local_files:
            df = pd.read_csv(file_path)
            
            # 변동률 계산
            df['Close'] = df['Close'].astype(float)
            df['Prev_Close'] = df['Close'].shift(1)
            df['Change'] = (df['Close'] - df['Prev_Close']) / df['Prev_Close'] * 100

            # 변동률이 10% 이상인 날짜 찾기
            high_volatility = df[df['Change'].abs() > 10]
            for _, row in high_volatility.iterrows():
                high_volatility_days.append({
                    'Code': df['Code'].iloc[0],
                    'Date': row['Date'],
                    'Change': row['Change']
                })

        # 결과 출력
        for item in high_volatility_days:
            logging.info(f"Code: {item['Code']}, Date: {item['Date']}, Change: {item['Change']}%")
        
        return high_volatility_days

    top_3_codes = read_csv_from_s3()
    local_files = fetch_csv_files(top_3_codes)
    find_high_volatility_days(local_files)

dag = stock_top3()
