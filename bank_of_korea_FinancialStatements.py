import csv
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os

# 기본 DAG 인자 설정
default_args = {
    'owner': 'seungjun',  # DAG 소유자
    'depends_on_past': False,  # 이전 DAG 실행의 성공 여부에 의존하지 않음
    'email_on_failure': False,  # 실패 시 이메일 알림 비활성화
    'email_on_retry': False,  # 재시도 시 이메일 알림 비활성화
    'retries': 1,  # 실패 시 재시도 횟수
    'retry_delay': timedelta(minutes=5),  # 재시도 간격
}

@dag(
    dag_id='financial_statements_etl_dag',
    default_args=default_args,
    description='한국 재무제표 데이터를 수집 및 가공하여 S3에 업로드하는 ETL DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def financial_statements_etl():
    """한국 재무제표 데이터를 수집, 가공하여 S3에 저장하는 ETL 작업을 정의하는 DAG"""

    @task(task_id="fetch_data")
    def fetch_data():
        """한국 재무제표 데이터를 API로부터 가져와 로컬에 CSV 파일로 저장"""
        end_date = datetime.now().strftime('%Y')

        url = (
            "https://ecos.bok.or.kr/api/StatisticSearch/"
            "GZJ2WT8Y559OMJKLPMRQ/json/kr/1/99999/501Y001/A/2014/{end_date}"
        ).format(end_date=end_date)
        
        response = requests.get(url)
        data = response.json()

        # 데이터를 CSV 파일로 저장
        file_path = '/tmp/KeyStatisticList.csv'
        fieldnames = [
            "STAT_CODE", "STAT_NAME", "ITEM_CODE1", "ITEM_NAME1", "ITEM_CODE2", 
            "ITEM_NAME2", "ITEM_CODE3", "ITEM_NAME3", "ITEM_CODE4", "ITEM_NAME4", 
            "UNIT_NAME", "WGT", "TIME", "DATA_VALUE"
        ]
        
        with open(file_path, mode='w', newline='', encoding='utf-8-sig') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for row in data.get('StatisticSearch', {}).get('row', []):
                writer.writerow({field: row.get(field) for field in fieldnames})

        return file_path

    @task(task_id="process_data")
    def process_data(file_path: str):
        """수집된 재무제표 데이터를 전처리하여 가공된 CSV 파일로 저장"""
        df = pd.read_csv(file_path, encoding='utf-8-sig')

        # 열 이름을 재정의하고, 데이터 처리
        df = df.rename(columns={"TIME": "Date", "DATA_VALUE": "FinancialData"})
        df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
        df = df[['Date', 'ITEM_NAME1', 'FinancialData']]

        # 데이터가 긴 형식에서 넓은 형식으로 피벗
        df_pivot = df.pivot_table(
            index='Date', 
            columns='ITEM_NAME1', 
            values='FinancialData', 
            aggfunc='first'
        ).reset_index()

        # 처리된 데이터를 새로운 CSV 파일로 저장
        processed_file_path = '/tmp/ProcessedFinancialStatements.csv'
        df_pivot.to_csv(processed_file_path, index=False, encoding='utf-8-sig')

        return processed_file_path

    @task(task_id="upload_to_s3")
    def upload_to_s3(file_path: str):
        """로컬에 저장된 재무제표 CSV 파일을 S3에 업로드"""
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        s3_key = f'newb_data/bank_of_korea/processed/FinancialStatements.csv'
        s3_hook.load_file(file_path, s3_key, bucket_name=s3_bucket, replace=True)

    # DAG 실행 순서 정의
    raw_file_path = fetch_data()
    processed_file_path = process_data(raw_file_path)
    upload_to_s3(raw_file_path)
    upload_to_s3(processed_file_path)

# DAG 인스턴스 생성
dag = financial_statements_etl()
