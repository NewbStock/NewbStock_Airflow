import csv
import requests
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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
        file_path = '/tmp/KeyStatisticList.csv'
        fieldnames = ["STAT_CODE", "STAT_NAME", "ITEM_CODE1", "ITEM_NAME1", "ITEM_CODE2", "ITEM_NAME2", 
                      "ITEM_CODE3", "ITEM_NAME3", "ITEM_CODE4", "ITEM_NAME4", "UNIT_NAME", "WGT", "TIME", "DATA_VALUE"]
        
        with open(file_path, mode='w', newline='', encoding='utf-8-sig') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for row in data.get('StatisticSearch', {}).get('row', []):
                writer.writerow({field: row.get(field) for field in fieldnames})

        return file_path


    @task(task_id="upload_to_s3")
    def upload_to_s3(file_path: str):
        # S3에 파일 업로드
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_bucket = 'team-won-2-bucket'
        s3_key = 'newb_data/NewsSentiment.csv'
        s3_hook.load_file(file_path, s3_key, bucket_name=s3_bucket, replace=True)

    # 태스크 실행 및 의존성 설정
    file_paths = fetch_data()
    upload_to_s3(file_paths)

dag = news_sentiment_etl()
