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
    dag_id='financial_statements_etl_dag',
    default_args=default_args,
    description='한국 재무제표 데이터를 수집 및 가공하여 S3에 업로드하는 ETL DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def financial_statements_etl():
    """한국 재무제표 데이터를 수집, 가공하여 S3에 저장하는 ETL 작업을 정의하는 DAG"""

    @task(task_id="fetch_data_part1")
    def fetch_data_part1():
        """첫 번째 기간(1990-2000) 동안의 재무제표 데이터를 가져와 로컬에 CSV로 저장"""
        url = (
            "https://ecos.bok.or.kr/api/StatisticSearch/"
            "GZJ2WT8Y559OMJKLPMRQ/json/kr/1/99999/501Y001/A/1990/2000"
        )
        response = requests.get(url)
        data = response.json()

        # 데이터를 CSV 파일로 저장
        file_path = '/tmp/KeyStatisticList_part1.csv'
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

    @task(task_id="fetch_data_part2")
    def fetch_data_part2():
        """두 번째 기간(2000-2010) 동안의 재무제표 데이터를 가져와 로컬에 CSV로 저장"""
        url = (
            "https://ecos.bok.or.kr/api/StatisticSearch/"
            "GZJ2WT8Y559OMJKLPMRQ/json/kr/1/99999/501Y001/A/2000/2010"
        )
        response = requests.get(url)
        data = response.json()

        # 데이터를 CSV 파일로 저장
        file_path = '/tmp/KeyStatisticList_part2.csv'
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

    @task(task_id="fetch_data_part3")
    def fetch_data_part3():
        """세 번째 기간(2010-2020) 동안의 재무제표 데이터를 가져와 로컬에 CSV로 저장"""
        url = (
            "https://ecos.bok.or.kr/api/StatisticSearch/"
            "GZJ2WT8Y559OMJKLPMRQ/json/kr/1/99999/501Y001/A/2010/2020"
        )
        response = requests.get(url)
        data = response.json()

        # 데이터를 CSV 파일로 저장
        file_path = '/tmp/KeyStatisticList_part3.csv'
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

    @task(task_id="fetch_data_part4")
    def fetch_data_part4():
        """네 번째 기간(2020-2024) 동안의 재무제표 데이터를 가져와 로컬에 CSV로 저장"""
        end_date = datetime.now().strftime('%Y')
        url = (
            "https://ecos.bok.or.kr/api/StatisticSearch/"
            "GZJ2WT8Y559OMJKLPMRQ/json/kr/1/99999/501Y001/A/2020/{end_date}"
        ).format(end_date=end_date)
        
        response = requests.get(url)
        data = response.json()

        # 데이터를 CSV 파일로 저장
        file_path = '/tmp/KeyStatisticList_part4.csv'
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

    @task(task_id="upload_raw_to_s3")
    def upload_raw_to_s3(file_path1: str, file_path2: str, file_path3: str, file_path4: str):
        """네 기간의 원시 데이터를 S3 버킷에 업로드"""
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        
        s3_key1 = 'newb_data/bank_of_korea/raw_data/FinancialStatements_part1.csv'
        s3_hook.load_file(file_path1, s3_key1, bucket_name=s3_bucket, replace=True)
        
        s3_key2 = 'newb_data/bank_of_korea/raw_data/FinancialStatements_part2.csv'
        s3_hook.load_file(file_path2, s3_key2, bucket_name=s3_bucket, replace=True)
        
        s3_key3 = 'newb_data/bank_of_korea/raw_data/FinancialStatements_part3.csv'
        s3_hook.load_file(file_path3, s3_key3, bucket_name=s3_bucket, replace=True)
        
        s3_key4 = 'newb_data/bank_of_korea/raw_data/FinancialStatements_part4.csv'
        s3_hook.load_file(file_path4, s3_key4, bucket_name=s3_bucket, replace=True)
        
        return [f"s3://{s3_bucket}/{s3_key1}", f"s3://{s3_bucket}/{s3_key2}", f"s3://{s3_bucket}/{s3_key3}", f"s3://{s3_bucket}/{s3_key4}"]

    @task(task_id="process_data")
    def process_data(raw_s3_paths: list):
        """S3에서 원시 데이터를 다운로드하고 결합한 후 처리"""
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        dfs = []
        
        for raw_s3_path in raw_s3_paths:
            s3_bucket, s3_key = raw_s3_path.replace('s3://', '').split('/', 1)

            try:
                obj = s3_hook.get_key(s3_key, s3_bucket)
                csv_content = obj.get()["Body"].read()
                df = pd.read_csv(BytesIO(csv_content), encoding='utf-8-sig')
                dfs.append(df)
            except Exception as e:
                logging.error(f"Error reading file {s3_key} from S3: {e}")
                raise

        # 데이터프레임 합치기
        combined_df = pd.concat(dfs)

        # 필요한 데이터 전처리 수행
        combined_df['Date'] = pd.to_datetime(combined_df['TIME'], format='%Y')
        df_pivot = combined_df.pivot_table(
            index='Date', 
            columns='ITEM_NAME1', 
            values='DATA_VALUE', 
            aggfunc='first'
        ).reset_index()

        processed_file_path = '/tmp/ProcessedFinancialStatements.csv'
        df_pivot.to_csv(processed_file_path, index=False, encoding='utf-8-sig')

        return processed_file_path

    @task(task_id="upload_processed_to_s3")
    def upload_processed_to_s3(file_path: str):
        """처리된 데이터를 S3 버킷에 업로드"""
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        s3_key = 'newb_data/bank_of_korea/processed/FinancialStatements.csv'
        s3_hook.load_file(file_path, s3_key, bucket_name=s3_bucket, replace=True)
        return f"s3://{s3_bucket}/{s3_key}"

    # DAG 실행 순서 정의
    file_path1 = fetch_data_part1()
    file_path2 = fetch_data_part2()
    file_path3 = fetch_data_part3()
    file_path4 = fetch_data_part4()
    raw_s3_paths = upload_raw_to_s3(file_path1, file_path2, file_path3, file_path4)
    processed_file_path = process_data(raw_s3_paths)
    upload_processed_to_s3(processed_file_path)

# DAG 인스턴스 생성
dag = financial_statements_etl()
