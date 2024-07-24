"""
한국경제 코리아마켓 웹사이트에서 제공하는 시가총액 정보
"https://markets.hankyung.com/index-info/marketcap"
시가총액 Top100 기업명과 종목코드'kr_top100.csv' 파일 생성해서 S3에 저장
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
from datetime import timedelta
import pandas as pd
import time


def get_kr_marketcap_top100():
    # 파일 경로 실제 사용하는 버킷, 파일 경로로 변경 필요
    bucket_name = 'team-won-2-bucket'
    output_key = 's3://team-won-2-bucket/kr_stock_data/kr_top100.csv'

    # 크롬 백그라운드 실행
    options = webdriver.ChromeOptions()
    options.add_argument("headless")

    # Selenium 실행
    with webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options) as driver:
        url = 'https://markets.hankyung.com/index-info/marketcap'
        driver.get(url)
        driver.implicitly_wait(10) # 페이지 렌더링 대기

        # 기업명과 종목코드 크롤링
        companies = []
        codes = []
        
        for page in range(1, 3):  # 1 페이지와 2 페이지 크롤링
            if page > 1: # 페이지 이동
                next_button = driver.find_element(By.XPATH, '//*[@id="container"]/div/div/div[2]/div[2]/div/a[2]')
                next_button.click()
                time.sleep(2)  # 페이지가 로드될 시간을 주기 위해 잠시 대기

            # 테이블의 모든 행을 찾음
            rows = driver.find_elements(By.CSS_SELECTOR, "tbody tr")

            for row in rows:
                # 각 행의 첫 번째 열 (기업명)과 두 번째 열 (종목코드)을 찾음
                try:
                    company_name = row.find_element(By.CSS_SELECTOR, "p.stock-name.ellip a").text
                    #code = row.find_element(By.CSS_SELECTOR, "p.code.txt-num.ellip").text.strip()
                    code = row.find_element(By.CSS_SELECTOR, "p.code.txt-num.ellip").text
                    companies.append(company_name)
                    codes.append(code)
                except Exception as e:
                    print(f"Error processing row: {e}")

            # 크롤링한 데이터를 데이터프레임으로 변환
            df = pd.DataFrame({
                'CompanyName': companies,
                'CompanyCode': codes
            })

        # DataFrame을 CSV 형식으로 변환
        df.to_csv('kr_top100.csv', index=False, encoding='utf-8-sig')

        # WebDriver 종료
        driver.quit()

    # S3에 파일 업로드
    s3_hook = S3Hook(aws_conn_id='s3_conn')   # connection 생성 후 변경 필요
    csv_buffer = pd.compat.StringIO()
    s3_hook.load_string(csv_buffer.getvalue(), output_key, bucket_name, replace=True)
    print("CSV 파일이 성공적으로 저장되었습니다.")


default_args = {
    'owner': 'kyoungyeon',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id = 'get_kr_marketcap_top100',
    default_args=default_args,
    description='Daily crawling Korean Market Marketcap Top100 data to S3',
    catchup = False,
    schedule_interval='* 18 * * *',  # 장 마감 후, 평일(1,2,3,4,5) 저녁 6시 
      
)

process_kospi_data_task = PythonOperator(
    task_id='get_kr_marketcap_top100',
    python_callable=get_kr_marketcap_top100,
    dag=dag,
)


