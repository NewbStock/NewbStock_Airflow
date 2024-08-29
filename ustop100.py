# airflow library
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from datetime import datetime, timedelta, date
import logging
import time
import FinanceDataReader as fdr
import io
import pandas as pd

# crawling library
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import yfinance as yf

def USStockTop100():
    # 크롬 백그라운드 실행
    options = webdriver.ChromeOptions()
    options.add_argument("headless")

    # 셀레니움 실행
    remote_webdriver = 'remote_chromedriver'
    with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options) as driver:
        url = 'https://earnings.kr/marketcap'
        driver.get(url)
        driver.implicitly_wait(10) # 페이지 렌더링 대기
        name = driver.find_elements(By.CLASS_NAME, 'name')
        code = driver.find_elements(By.CLASS_NAME, 'symbol')
        name_list = []
        code_list = []
        ex_dict = {
            'NMS': 'NAS',
            'NYQ': 'NYS',
        }
        ex_list = []

        length = len(name)
        for i in range(length):
            name_list.append(name[i].text.replace("'", '').replace('"', ''))
            code_list.append(code[i].text)

        for company_code in code_list:
            try:
                company_info = yf.Ticker(company_code)
                ex = company_info.history_metadata['exchangeName']
                ex_list.append(ex_dict[ex])
            except Exception as ex:
                # yfinance에 데이터가 없을 때
                ex_list.append('baddata')
                print('data error', ex)

        ranking_list = [i for i in range(1, 101)]

        df = pd.DataFrame({
            'ranking': ranking_list,
            'name' : name_list,
            'code' : code_list,
            'excode' : ex_list,
        })
        logging.info("crawling done")

        # DataFrame을 CSV 형식으로 변환
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        csv_buffer.seek(0)
    key = "us_stock_data/us_stock_top100.csv"
    bucket_name = "team-won-2-bucket"
    s3_hook = S3Hook(aws_conn_id='s3_conn')   # connection 생성 후 변경 필요
    s3_hook.load_string(csv_buffer.getvalue(), key, bucket_name, replace=True)
    csv_buffer.close()
    logging.info("s3 upload done")

default_args = {
    'owner': 'joonghyeon',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 15),
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}
with DAG(
    dag_id='us_stock_top100',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    schedule='0 11 * * 2-6', # 적당히 조절
) as dag:
    crawling_task = PythonOperator(
        task_id='crawling_us_top100_list',
        python_callable=USStockTop100,
        dag=dag
    )
    upload_to_redshift_task = GlueJobOperator(
        task_id='upload_to_redshift',
        job_name='newbstock_top100_s3_to_redshift',
        script_location='s3://team-won-2-glue-bucket/scripts/newbstock_top100_s3_to_redshift.py',  # Glue Job에 필요한 스크립트 경로
        region_name='ap-northeast-2',
        iam_role_name='newbstock_glue_role',
        dag=dag
    )
    crawling_task >> upload_to_redshift_task
    