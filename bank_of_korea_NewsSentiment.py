import csv
import requests
import logging
from datetime import datetime, timedelta
from io import BytesIO
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd


default_args = {
    'owner': 'seungjun',  
    'depends_on_past': False,  
    'email_on_failure': False, 
    'email_on_retry': False,  
    'retries': 1,  
    'retry_delay': timedelta(minutes=5),  
}

@dag(
    dag_id='news_sentiment_etl_dag',
    default_args=default_args,
    description='뉴스 심리 지수 데이터 수집 및 처리',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def news_sentiment_etl():
    """뉴스 심리 지수 데이터를 수집, 처리 및 S3에 업로드하는 ETL 작업"""

    @task(task_id="fetch_data")
    def fetch_data():
        """API로부터 뉴스 심리 지수 데이터를 가져와 CSV 파일로 저장"""
        end_date = datetime.now().strftime('%Y%m%d')
        url = (
            f"https://ecos.bok.or.kr/api/StatisticSearch/"
            f"GZJ2WT8Y559OMJKLPMRQ/json/kr/1/99999/521Y001/D/19900101/{end_date}"
        )
        response = requests.get(url)
        data = response.json()

        # 데이터를 CSV 파일로 저장
        file_path = '/tmp/NewsSentiment.csv'
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
    def upload_raw_to_s3(file_path: str):
        """로컬에 저장된 원시 데이터를 S3 버킷에 업로드"""
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        s3_key = 'newb_data/bank_of_korea/raw_data/NewsSentiment.csv'
        s3_hook.load_file(file_path, s3_key, bucket_name=s3_bucket, replace=True)
        return f"s3://{s3_bucket}/{s3_key}"

    @task(task_id="process_data")
    def process_data(raw_s3_path: str):
        """S3에서 원시 데이터를 다운로드하여 처리 후 다시 S3에 저장"""
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket, s3_key = raw_s3_path.replace('s3://', '').split('/', 1)

        try:
            obj = s3_hook.get_key(s3_key, s3_bucket)
            csv_content = obj.get()["Body"].read()
            df = pd.read_csv(BytesIO(csv_content), encoding='utf-8-sig')

            # 필요한 데이터 전처리 수행
            df = df.rename(columns={"DATA_VALUE": "NewsSentiment"})
            df = df[['TIME', 'NewsSentiment']]

            processed_file_path = '/tmp/ProcessedNewsSentimentData.csv'
            df.to_csv(processed_file_path, index=False, encoding='utf-8-sig')

            return processed_file_path
        except Exception as e:
            logging.error(f"Error reading file {s3_key} from S3: {e}")
            raise

    @task(task_id="upload_processed_to_s3")
    def upload_processed_to_s3(file_path: str):
        """처리된 데이터를 S3 버킷에 업로드"""
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-bucket'
        s3_key = 'newb_data/bank_of_korea/processed/ProcessedNewsSentimentData.csv'
        s3_hook.load_file(file_path, s3_key, bucket_name=s3_bucket, replace=True)
        processed_s3_path = f"s3://{s3_bucket}/{s3_key}"

        return processed_s3_path
    
    @task(task_id="truncate_table")
    def truncate_table():
        """
        Redshift 테이블에서 데이터를 삭제합니다.
        """
        redshift_conn_id = 'redshift_conn'
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
        
        truncate_sql = "TRUNCATE TABLE public.newssentiment;"
        
        try:
            redshift_hook.run(truncate_sql)
            logging.info("테이블을 성공적으로 비웠습니다.")
        except Exception as e:
            logging.error(f"테이블을 비우는 중 오류 발생: {e}")
    
    @task(task_id="load_to_redshift")
    def load_to_redshift(processed_s3_path: str):
        """S3에서 처리된 파일을 Redshift로 로드"""
        redshift_conn_id = 'redshift_conn'
        aws_conn_id = 'aws_default'
        redshift_table = 'public.newssentiment'
        
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
    file_path = fetch_data()
    raw_s3_path = upload_raw_to_s3(file_path)
    processed_file_path = process_data(raw_s3_path)
    processed_s3_path = upload_processed_to_s3(processed_file_path)
    load_to_redshift(processed_s3_path)
    truncate_table() >> load_to_redshift(processed_s3_path)

# DAG 인스턴스 생성
dag = news_sentiment_etl()

