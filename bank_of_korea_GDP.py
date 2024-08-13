import csv
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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

@dag(
    dag_id='gdp_etl_dag',
    default_args=default_args,
    description='미국, 중국, 일본, 한국의 GDP 데이터를 수집 및 가공하여 S3에 업로드하는 ETL DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def gdp_etl():
    """미국, 중국, 일본, 한국의 GDP 데이터를 수집, 가공하여 S3에 저장하는 ETL 작업을 정의하는 DAG"""

    @task(task_id="fetch_data")
    def fetch_data():
        """각 국가의 GDP 데이터를 수집하여 로컬에 CSV 파일로 저장"""
        base_url = (
            "https://ecos.bok.or.kr/api/StatisticSearch/"
            "GZJ2WT8Y559OMJKLPMRQ/json/kr/1/1000/902Y016/A/1900/{end_year}/{country_code}"
        )
        end_year = datetime.now().strftime('%Y')

        countries = {
            "Korea": "KOR",
            "Japan": "JPN",
            "USA": "USA",
            "China": "CHN"
        }

        file_paths = []
        for country_name, country_code in countries.items():
            url = base_url.format(end_year=end_year, country_code=country_code)
            response = requests.get(url)
            data = response.json()

            file_path = f'/tmp/{country_name}.csv'
            fieldnames = [
                "STAT_CODE", "STAT_NAME", "ITEM_CODE1", "ITEM_NAME1", "ITEM_CODE2", 
                "ITEM_NAME2", "ITEM_CODE3", "ITEM_NAME3", "ITEM_CODE4", "ITEM_NAME4", 
                "UNIT_NAME", "WGT", "TIME", "DATA_VALUE"
            ]
            
            with open(file_path, mode='w', newline='', encoding='utf-8-sig') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                for row in data['StatisticSearch']['row']:
                    writer.writerow({field: row.get(field) for field in fieldnames})

            file_paths.append(file_path)

        return file_paths

    @task(task_id="process_data")
    def process_data(file_paths: list):
        """수집된 GDP 데이터를 가공하여 처리된 CSV 파일로 저장"""
        processed_file_paths = []

        for file_path in file_paths:
            country_name = os.path.basename(file_path).split('.')[0]

            # CSV 파일 읽기
            df = pd.read_csv(file_path)

            # 데이터 가공
            df_grouped = df.groupby('TIME')['DATA_VALUE'].sum().reset_index()
            df_grouped = df_grouped.rename(columns={'TIME': 'Date', 'DATA_VALUE': 'GDP'})
            
            # 처리된 데이터를 새로운 CSV 파일로 저장
            processed_file_path = f'/tmp/Processed_{country_name}.csv'
            df_grouped.to_csv(processed_file_path, index=False, encoding='utf-8-sig')
            processed_file_paths.append(processed_file_path)

        return processed_file_paths

    @task(task_id="upload_to_s3")
    def upload_to_s3(file_paths: list):
        """로컬에 저장된 CSV 파일들을 S3에 업로드"""
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'

        for file_path in file_paths:
            country_name = os.path.basename(file_path).split('.')[0]
            s3_key = f'newb_data/bank_of_korea/processed/gdp/{country_name}_GDP.csv'
            s3_hook.load_file(file_path, s3_key, bucket_name=s3_bucket, replace=True)

    # DAG 실행 순서 정의
    raw_file_paths = fetch_data()
    processed_file_paths = process_data(raw_file_paths)
    upload_to_s3(raw_file_paths)
    upload_to_s3(processed_file_paths)

# DAG 인스턴스 생성
dag = gdp_etl()
