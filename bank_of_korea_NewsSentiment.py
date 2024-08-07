import csv
import requests
import logging
from datetime import datetime, timedelta
from io import StringIO, BytesIO
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import os

# 기본 인자 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='news_sentiment_etl_dag',
    default_args=default_args,
    description='뉴스 심리 지수',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def news_sentiment_etl():

    @task(task_id="fetch_data")
    def fetch_data():
        end_date = datetime.now().strftime('%Y%m%d')

        url = f"https://ecos.bok.or.kr/api/StatisticSearch/GZJ2WT8Y559OMJKLPMRQ/json/kr/1/99999/521Y001/D/19900101/{end_date}"
        response = requests.get(url)
        data = response.json()

        # 데이터를 CSV 파일로 저장
        file_path = '/tmp/NewsSentiment.csv'
        fieldnames = ["STAT_CODE", "STAT_NAME", "ITEM_CODE1", "ITEM_NAME1", "ITEM_CODE2", "ITEM_NAME2", 
                      "ITEM_CODE3", "ITEM_NAME3", "ITEM_CODE4", "ITEM_NAME4", "UNIT_NAME", "WGT", "TIME", "DATA_VALUE"]
        
        with open(file_path, mode='w', newline='', encoding='utf-8-sig') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for row in data.get('StatisticSearch', {}).get('row', []):
                writer.writerow({field: row.get(field) for field in fieldnames})

        return file_path

    @task(task_id="upload_raw_to_s3")
    def upload_raw_to_s3(file_path: str):
        # S3에 파일 업로드
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        s3_key = 'newb_data/bank_of_korea/raw_data/NewsSentiment.csv'
        s3_hook.load_file(file_path, s3_key, bucket_name=s3_bucket, replace=True)
        return f"s3://{s3_bucket}/{s3_key}"

    @task(task_id="process_data")
    def process_data(raw_s3_path: str):
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket, s3_key = raw_s3_path.replace('s3://', '').split('/', 1)

        try:
            obj = s3_hook.get_key(s3_key, s3_bucket)
            csv_content = obj.get()["Body"].read()
            df = pd.read_csv(BytesIO(csv_content), encoding='utf-8-sig')

            # 필요한 데이터 전처리 수행
            df['TIME'] = pd.to_datetime(df['TIME'], format='%Y%m%d')
            df.set_index('TIME', inplace=True)
            df_resampled = df.resample('M').mean().reset_index()

            processed_file_path = '/tmp/ProcessedNewsSentimentData.csv'
            df_resampled.to_csv(processed_file_path, index=False, encoding='utf-8-sig')

            return processed_file_path
        except Exception as e:
            logging.error(f"Error reading file {s3_key} from S3: {e}")
            raise

    @task(task_id="upload_processed_to_s3")
    def upload_processed_to_s3(file_path: str):
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        s3_key = 'newb_data/bank_of_korea/processed/ProcessedNewsSentimentData.csv'
        s3_hook.load_file(file_path, s3_key, bucket_name=s3_bucket, replace=True)
        return f"s3://{s3_bucket}/{s3_key}"

    file_path = fetch_data()
    raw_s3_path = upload_raw_to_s3(file_path)
    processed_file_path = process_data(raw_s3_path)
    upload_processed_to_s3(processed_file_path)

dag = news_sentiment_etl()
