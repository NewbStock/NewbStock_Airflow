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
    dag_id='gdp_etl_dag',
    default_args=default_args,
    description='미국,중국,일본,한국 gdp 데이터 가져오기',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def gdp_etl():

    @task(task_id="fetch_data")
    def fetch_data():
        base_url = "https://ecos.bok.or.kr/api/StatisticSearch/GZJ2WT8Y559OMJKLPMRQ/json/kr/1/1000/902Y016/A/1900/{end_year}/{country_code}"
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

            file_path = f'/tmp/bank_of_kroea/GDP/{country_name}.csv'
            fieldnames = ["STAT_CODE", "STAT_NAME", "ITEM_CODE1", "ITEM_NAME1", "ITEM_CODE2", "ITEM_NAME2", 
                            "ITEM_CODE3", "ITEM_NAME3", "ITEM_CODE4", "ITEM_NAME4", "UNIT_NAME", "WGT", "TIME", "DATA_VALUE"]
            
            with open(file_path, mode='w', newline='', encoding='utf-8-sig') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                for row in data['StatisticSearch']['row']:
                    writer.writerow({field: row.get(field) for field in fieldnames})

                file_paths.append(file_path)

        return file_paths

    @task(task_id="upload_to_s3")
    def upload_to_s3(file_paths: list):
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_bucket = 'team-won-2-bucket'

        for file_path in file_paths:
            country_name = os.path.basename(file_path).split('_')[0]
            s3_key = f'newb_data/{country_name}_GDP.csv'
            s3_hook.load_file(file_path, s3_key, bucket_name=s3_bucket, replace=True)

    # 태스크 실행 및 의존성 설정
    file_paths = fetch_data()
    upload_to_s3(file_paths)

dag = gdp_etl()
