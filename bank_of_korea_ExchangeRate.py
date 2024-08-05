import csv
import requests
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='exchange_rate_etl_dag',
    default_args=default_args,
    description='Fetch and process exchange rate data for beginner stock investors',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def exchange_rate_etl():
    @task(task_id="fetch_data")
    def fetch_data():
        end_date = datetime.now().strftime('%Y%m%d')

        url = f"https://ecos.bok.or.kr/api/StatisticSearch/GZJ2WT8Y559OMJKLPMRQ/json/kr/1/100000/731Y003/D/19210101/{end_date}"
        response = requests.get(url)
        data = response.json()

        # 데이터를 CSV 파일로 저장
        file_path = '/tmp/bank_of_kroea/KeyStatisticList.csv'
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
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_bucket = 'team-won-2-bucket'
        s3_key = 'newb_data/ExchangeRate.csv'
        s3_hook.load_file(file_path, s3_key, bucket_name=s3_bucket, replace=True)
        return f"s3://{s3_bucket}/{s3_key}"


    file_paths = fetch_data()
    upload_to_s3(file_paths)

dag = exchange_rate_etl()
