from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime, timedelta, date

import logging
import boto3
import time
import FinanceDataReader as fdr
import io
import psycopg2
# crawling library
import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait

# spark library
# from pyspark.sql import *
# from pyspark.sql.functions import *

# redshift 연결
def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# csv 파일로 저장
@task
def USStockTop100():
    # 크롬 백그라운드 실행
    options = webdriver.ChromeOptions()
    options.add_argument("headless")
    
    # 셀레니움 실행
    with webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options) as driver:
        url = 'https://earnings.kr/marketcap'
        driver.get(url)
        driver.implicitly_wait(10) # 페이지 렌더링 대기

        company_name_list = []
        name = driver.find_elements(By.CLASS_NAME, 'name')
        code = driver.find_elements(By.CLASS_NAME, 'symbol')
        
        length = len(name)
        for i in range(length):
            company_name_list.append([name[i].text, code[i].text]) # 0 : name, 1 : 
            
        df = pd.DataFrame(company_name_list)
    
    # s3에 적재
    ACCESS_KEY = Variable.get("AWS_AUTHORIZATION_KEY_ID")
    SECRET_KEY = Variable.get("AWS_SECRET_KEY")
    
    s3_client = boto3.client(
        service_name='s3',
        region_name="ap-northeast-2",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    bucket_name = "team-won-2-bucket"
    key = "us_stock_top100.csv"
    s3_client.put_object(
        Body=df.to_csv(index=False, encoding="utf-8-sig").encode(),
        Bucket=bucket_name,
        Key="us_stock_data/" + key
    )
    logging.info("s3 upload done")
    
@task
def api_get_upload_s3():
    ACCESS_KEY = Variable.get("AWS_AUTHORIZATION_KEY_ID")
    SECRET_KEY = Variable.get("AWS_SECRET_KEY")
    
    bucket_name = "team-won-2-bucket"
    prefix = "us_stock_data/history/"
    
    s3_client = boto3.client(
        service_name='s3',
        region_name="ap-northeast-2",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    # s3에 적재된 파일 리스트 추출
    obj_list = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)
    contents_list = obj_list['Contents']
    file_list = []
    
    for content in contents_list:
        file_name = content['Key'].replace(prefix, '')
        file_list.append(file_name) # APPL.csv 형식
    
    # 현재 top100 리스트
    data = pd.read_csv(
        io.BytesIO(
            s3_client.get_object(
                Bucket=bucket_name,
                Key="us_stock_data/us_stock_top100.csv"
            )['Body'].read()
        )
    )
    company_name_list = data['name']
    company_code_list = data['code']
    
    for file in file_list:
        code = file.replace(".csv", '')
        if code not in company_code_list:
            response = s3_client.delete_object(
                Bucket=bucket_name,
                Key=prefix + file
            )
            print(response)
    
    update_dict = {}
    
    for i in range(len(company_code_list)):
        company_name = company_name_list[i] # 회사 이름
        company_code = company_code_list[i] # 회사 코드(심벌)
        
        file_name = company_code + ".csv"
        
        if file_name in file_list:
            # s3에서 파일 열기
            read_file = pd.read_csv(
                io.StringIO(
                    s3_client.get_object(
                        Bucket=bucket_name,
                        Key="us_stock_data/history/" + file_name
                    )['Body'].read()
                )
            )
            try:
                yesterday = date.today() - timedelta(days=1)
                hist = fdr.DataReader('YAHOO:' + company_code, yesterday)
                hist.insert(0, 'Code', company_code)
                hist.insert(0, 'Name', company_name)
                update_dict[company_code] = hist # APPL: dataframe
                
                save_file_name = pd.concat([read_file, hist], ignore_index=True)

                csv_buffer = io.StringIO()
                save_file_name.to_csv(csv_buffer, index=True)
                
                s3_client.put_object(
                    Body=csv_buffer.getvalue(),
                    Bucket=bucket_name,
                    Key=file_name
                )
                logging.info(company_name + " file s3 update done")
            except Exception as ex:
                print('data error', ex)
            
        else:
            try:
                hist = fdr.DataReader('YAHOO:' + company_code)
                hist.insert(0, 'Code', company_code)
                hist.insert(0, 'Name', company_name)
                save_file_name = prefix + file_name
                
                s3_client.put_object(
                    Body=hist.to_csv(index=True).encode(),
                    Bucket=bucket_name,
                    Key=save_file_name
                )
                logging.info(company_name + " file s3 upload done")
                
            except Exception as ex:
                print('data error', ex)
        time.sleep(5)
    
    return update_dict
    

@task
def etl(update_dict, schema, table):
    # extract
    bucket_name = "team-won-2-bucket"
    prefix = "us_stock_data/history/"
    
    ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
    SECRET_KEY = Variable.get("AWS_SECRET_KEY")
    
    # s3 클라이언트 연결
    s3_client = boto3.client(
        service_name='s3',
        region_name="ap-northeast-2",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    # redshift 연결
    cur = get_Redshift_connection()
    # 테이블 생성
    cur.execute = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
        date DATE,
        name VARCHAR(100),
        code VARCHAR(20),
        open NUMERIC(10, 2)
    );"""
    
    # etl
    obj_list = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)
    contents_list = obj_list['Contents']
    
    """
    records = [
        [ Date, Name, Code, Open],
        [ 2022-01-01, Apple Inc., AAPL, 123.12],
        [ 2022-01-01, Apple Inc., AAPL, 234.12], 
        [ 2022-01-01, Apple Inc., AAPL, 556.31],
        ...
    ]
    """
    for content in contents_list:
        file_name = content.replace(prefix, '').replace('csv', '')
        if file_name in update_dict:
            hist = update_dict[file_name].iloc[0]
            try:
                date_var = datetime.strptime(hist['Date'], '%Y-%m-%d').date()
                name_var = hist['Name']
                code_var = hist['Code']
                open_var = hist['Open']
                cur.execute(f"""INSERT INTO stock_data(date, name, code, open) VALUES({date_var}, {name_var}, {code_var}, {open_var})""")
            except (Exception, psycopg2.DatabaseError) as error:
                    print(error)
                    cur.execute("ROLLBACK;")
            logging.info("update done")
            
        else:
            read_file = pd.read_csv(
                io.StringIO(
                    s3_client.get_object(
                        Bucket=bucket_name,
                        Key=content
                    )['Body'].read()
                )
            )
            hist = read_file[['Date', 'Name', 'Code', 'Open']]
            date_list = list(hist['Date']) # 날짜
            name_list = list(hist['Name']) # 회사명
            code_list = list(hist['Code']) # 회사코드
            open_list = list(hist['Open']) # 시가
            
            for i in range(len(date_list)):
                try:
                    cur.execute("BEGIN;")
                    date_var = datetime.strptime(date_list[i], '%Y-%m-%d').date()
                    name_var = name_list[i]
                    code_var = code_list[i]
                    open_var = open_list[i]
                    cur.execute(f"""INSERT INTO stock_data(date, name, code, open) VALUES({date_var}, {name_var}, {code_var}, {open_var})""")
                    cur.execute("COMMIT;")
                    cur.execute("END;") 
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)
                    cur.execute("ROLLBACK;")
            logging.info("load done")
        time.sleep(2)

with DAG(
    dag_id='us_stock_etl',
    start_date=datetime(2024, 7, 15), # 날짜가 미래인 경우 실행이 안됨
    schedule='0 13 * * TUE-SAT', # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:
    url = Variable.get("api_url")
    schema = ''   ## 자신의 스키마로 변경
    table = ''
    USStockTop100()
    update_dict = api_get_upload_s3()
    
