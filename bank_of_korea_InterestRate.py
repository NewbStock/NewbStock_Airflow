import csv
import requests
import logging
from datetime import datetime, timedelta
from io import BytesIO
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

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
    dag_id='interest_rate_etl_dag',
    default_args=default_args,
    description='한국 금리 데이터를 수집, 처리 및 S3에 업로드하는 ETL DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def interest_rate_etl():
    """한국 금리 데이터를 수집하여 S3에 저장하고 처리하는 ETL 작업을 정의하는 DAG"""

    @task(task_id="fetch_data_part1")
    def fetch_data_part1():
        """첫 번째 기간(1995-2010) 동안의 금리 데이터를 가져와 로컬에 CSV로 저장"""
        url = "https://ecos.bok.or.kr/api/StatisticSearch/GZJ2WT8Y559OMJKLPMRQ/json/kr/1/99999/817Y002/D/19950103/20100101"
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
        """두 번째 기간(2010-현재) 동안의 금리 데이터를 가져와 로컬에 CSV로 저장"""
        end_date = datetime.now().strftime('%Y%m%d')
        url = f"https://ecos.bok.or.kr/api/StatisticSearch/GZJ2WT8Y559OMJKLPMRQ/json/kr/1/99999/817Y002/D/20100102/{end_date}"
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

    @task(task_id="upload_raw_to_s3")
    def upload_raw_to_s3(file_path1: str, file_path2: str):
        """두 기간의 원시 데이터를 S3 버킷에 업로드"""
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        
        s3_key1 = 'newb_data/bank_of_korea/raw_data/InterRate_part1.csv'
        s3_hook.load_file(file_path1, s3_key1, bucket_name=s3_bucket, replace=True)
        
        s3_key2 = 'newb_data/bank_of_korea/raw_data/InterRate_part2.csv'
        s3_hook.load_file(file_path2, s3_key2, bucket_name=s3_bucket, replace=True)
        
        return [f"s3://{s3_bucket}/{s3_key1}", f"s3://{s3_bucket}/{s3_key2}"]

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

        # 필요한 데이터만 필터링
        selected_items = [
            "KOFR(공시 RFR)",
            "KORIBOR(12개월)",
            "국고채(10년)",
            "국고채(2년)",
            "국고채(5년)",
            "콜금리(1일, 은행증권금융차입)",
            "콜금리(1일, 전체거래)",
            "콜금리(1일, 중개회사거래)",
            "회사채(3년, AA-)",
            "회사채(3년, BBB-)"
        ]
        
        df_filtered = combined_df[combined_df['ITEM_NAME1'].isin(selected_items)]

        # 피벗 테이블 생성
        df_pivot = df_filtered.pivot_table(
            index='TIME', 
            columns='ITEM_NAME1', 
            values='DATA_VALUE', 
            aggfunc='first'
        ).reset_index()

        processed_file_path = '/tmp/ProcessedMarketInterestRateData.csv'
        df_pivot.to_csv(processed_file_path, index=False, encoding='utf-8-sig')

        return processed_file_path

    @task(task_id="upload_processed_to_s3")
    def upload_processed_to_s3(file_path: str):
        """처리된 데이터를 S3 버킷에 업로드"""
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        s3_key = 'newb_data/bank_of_korea/processed/ProcessedMarketInterestRateData.csv'
        s3_hook.load_file(file_path, s3_key, bucket_name=s3_bucket, replace=True)
        processed_s3_path = f"s3://{s3_bucket}/{s3_key}"\
        
        return processed_s3_path
    
    @task(task_id="load_to_redshift")
    def load_to_redshift(processed_s3_path: str):
        """S3에서 처리된 파일을 Redshift로 로드"""
        redshift_conn_id = 'redshift_conn'
        aws_conn_id = 'aws_default'
        redshift_table = 'public.market_interest_rates'
        
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        
        copy_sql = f"""
            COPY {redshift_table}
            FROM '{processed_s3_path}'
            ACCESS_KEY_ID '{s3_hook.get_credentials().access_key}'
            SECRET_ACCESS_KEY '{s3_hook.get_credentials().secret_key}'
            CSV
            IGNOREHEADER 1
            DELIMITER ','
            REGION 'ap-northeast-2';
        """
        try:
            redshift_hook.run(copy_sql)
            logging.info(f"{processed_s3_path} 데이터를 Redshift로 성공적으로 로드했습니다.")
        except Exception as e:
            logging.error(f"Redshift로 데이터를 로드하는 중 오류 발생: {e}")

    # DAG 실행 순서 정의
    file_path1 = fetch_data_part1()
    file_path2 = fetch_data_part2()
    raw_s3_paths = upload_raw_to_s3(file_path1, file_path2)
    processed_file_path = process_data(raw_s3_paths)
    processed_s3_path = upload_processed_to_s3(processed_file_path)
    load_to_redshift(processed_s3_path)

# DAG 인스턴스 생성
dag = interest_rate_etl()
