"""
S3에서 'kr_top100.csv' 데이터 가져오기
만약 {CompanyCode}.csv 파일이 S3에 없다면, FinanceDataReader에서 2000년 데이터부터 다 가져오기
파일이 이미 있다면, 당일 날짜의 데이터만 가져와서 원래 파일에 추가
"""
# pip install finance-datareader
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import logging
from io import StringIO


# AWS Redshift 연결
def get_redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_conn')
    conn = hook.get_conn()
    conn.autocommit = True  # default는 False
    return conn.cursor()


# S3에서 'kr_top100.csv' (오늘 시가총액 top100) 데이터 가져오기
"""
def get_kr_top100():
    bucket_name = 'team-won-2-bucket'
    input_key = 'kr_stock_data/kr_top100.csv'

    s3_hook = S3Hook(aws_conn_id='s3_conn') 

    try:
        # S3에서 파일 읽기
        kr_top100 = s3_hook.read_key(input_key, bucket_name)
        df_kr_top100 = pd.read_csv(StringIO(kr_top100))
        logging.info("Successfully read kr_top100.csv from S3")
        return df_kr_top100
    
    except Exception as e:
        logging.error(f"Error reading kr_top100.csv from S3: {str(e)}")
        return 
"""


# Redshift 테이블 kr_top100에서 distinct (name, code) 데이터 가져오기  
def get_disticnt_data():
    cur = get_redshift_connection()
    cur.execute("SELECT DISTINCT name, code FROM kr_top100")
    distinct_data = cur.fetchall()
    distinct_data = [{'name': row[0], 'code': row[1]} for row in distinct_data]
    logging.info("Successfully get distinct data from Redshift table kr_top100")
    return distinct_data


# S3에 존재하는 파일 이름 확인하기
def check_existing_files():
    bucket_name = 'team-won-2-bucket'
    prefix = 'kr_stock_data/stock_data/'
    
    s3_hook = S3Hook(aws_conn_id='s3_conn') 

    try:
        # S3에서 파일 목록 가져오기
        existing_files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)

        # CompanyCode 추출 (파일 이름에서 .csv 제거) e.g) 'Key': 'folder/file1.txt',
        existing_codes = [file.split('/')[-1].replace('.csv', '') for file in existing_files if file.endswith('.csv')]
        logging.info(f"Found {len(existing_codes)} existing company files in S3")
        return existing_codes
    
    except Exception as e:
        logging.error(f"Error listing files in S3: {str(e)}")
        return 


def update_redshift(update_df):
    # redshift 연결
    cur = get_redshift_connection()
    
    # 한국 주식 과거 데이터 테이블 date, name, code, open
    
    try:
        # Date, Open, High, Low, Close, Volume, Change, Updown, Comp, Amount, MarCap, Shares
        for index, row in update_df.iterrows():
            cur.execute("""
                INSERT INTO kr_stock_data (date, name, code, open_value)
                VALUES (%s, %s, %s, %s)
            """, (row['Date'], row['name'], row['code'], row['Open']))
    except Exception as e:
        logging.error(f"Error updating Redshift {update_df['name']} ({update_df['code']}): {str(e)}")


# S3에 시가총액 Top100 데이터 업데이트 및 생성
def update_stock_data():
    bucket_name = 'team-won-2-bucket'
    s3_hook = S3Hook(aws_conn_id='s3_conn') 

    # 한국 시장 시가총액 100위 데이터 가져오기
    # df_kr_top100 = get_kr_top100()
    # current_codes = set(df_kr_top100['CompanyCode'].astype(str).str.zfill(6))
    
    distinct_data = get_disticnt_data()

    # 이미 존재하는 주식 데이터 파일 확인 
    existing_codes = check_existing_files()

    # 현재 top 100 기업의 데이터 업데이트 또는 새로 생성
    for company in distinct_data:
        company_name = company['name']
        company_code = company['code']
        key = f'kr_stock_data/stock_data/{company_code}.csv'
        #company_code = str(company['code']).zfill(6)  # 'code' 컬럼의 값을 6자리로 맞추기 (앞에 0 추가)

        try:
            if company_code in existing_codes:
                # 기존 파일 읽어오기
                file_content = s3_hook.read_key(key, bucket_name)
                existing_df = pd.read_csv(StringIO(file_content), index_col=0, parse_dates=True)
                new_record = fdr.DataReader(f'KRX:{company_code}', datetime.today().date())  # UTC, KST 주의
                new_record.index.name = 'Date'  # 인덱스 이름 설정
                new_record.reset_index(inplace=True)  # 인덱스를 컬럼으로 변환
                df = pd.concat([existing_df, new_record])            
            else:
                df = fdr.DataReader(f'KRX:{company_code}', start = '2000-01-01')
                df.index.name = 'Date'  # 인덱스 이름 설정
                df.reset_index(inplace=True)  # 인덱스를 컬럼으로 변환
            
            # FinanceDataReader 컬럼
            # Date, Open, High, Low, Close, Volume, Change, Updown, Comp, Amount, MarCap, Shares
            if not df.empty:
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=True)
                """
                s3_hook.load_string(
                    string_data=csv_buffer.getvalue(),
                    key=key,
                    bucket_name=bucket_name,
                    replace=True
                )
                """
                
                logging.info(f"Successfully saved data for {company_name} {company_code}")

                # Redshift에 데이터 업데이트
                df['name'] = company_name
                df['code'] = company_code
                update_redshift(df)
            else:
                logging.info(f"Fail to get stock data for {company_name} {company_code}")
        except Exception as e:
            logging.error(f"Error processing {company_name} ({company_code}): {str(e)}")




default_args = {
    'owner': 'kyoungyeon',
    'depends_on_past': False,
    'retries': 1,
}

with DAG ( 
    dag_id='get_kr_stock_data',
    default_args=default_args,
    description='With FinanceDataReader, download, process, and upload KOSPI Marketcap Top100 data to S3',
    schedule_interval='30 9 * * 1-5',  # UTC 09:30 (KST 18:30), 월요일부터 금요일까지 장 마감 후  
    start_date=datetime(2024, 7, 15),  # 시작 날짜
    catchup=True,
) as dag:

    process_and_upload_stock_data_task = PythonOperator(
        task_id='update_kr_stock_data',
        python_callable=update_stock_data,
        dag=dag,
    )
