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
#from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
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


# S3에 시가총액 Top100 데이터 업데이트 및 생성
def update_stock_data():
    bucket_name = 'team-won-2-bucket'
    s3_hook = S3Hook(aws_conn_id='s3_conn') 
    
    # Redshift 테이블 kr_top100에서 distinct (name, code) 데이터 가져오기
    distinct_data = get_disticnt_data()
    logging.info("length of distinct data from Redshift Table 'kr_top100': {len(distinct_data)}")

    # S3에 이미 존재하는 주식 데이터 가져오기
    existing_codes = check_existing_files()
    logging.info("length of exisiting codes from S3 bucket: {len(distinct_data)}")

    # 현재 top 100 기업의 데이터 업데이트 또는 새로 생성
    for company in distinct_data:
        company_name = company['name']
        company_code = company['code']
        key = f'kr_stock_data/stock_data/{company_code}.csv'

        try:
            if company_code in existing_codes: 
                # 기존 파일 읽기
                file_content = s3_hook.read_key(key, bucket_name)
                existing_df = pd.read_csv(StringIO(file_content), index_col=0, parse_dates=True)
                new_record = fdr.DataReader(f'KRX:{company_code}', datetime.today().date())  # UTC, KST 주의
                df = pd.concat([existing_df, new_record])            
            else:
                df = fdr.DataReader(f'KRX:{company_code}', start_date = '2000-01-01')        
            

            df.index.name = 'Date'  # 인덱스 이름 설정
            df.reset_index(inplace=True)  # 인덱스를 컬럼으로 변환

            # FinanceDataReader 컬럼
            # Date, Open, High, Low, Close, Volume, Change, Updown, Comp, Amount, MarCap, Shares
            df['name'] = company_name
            df['code'] = company_code
            
            if not df.empty:
                key = f'kr_stock_data/stock_data/{company_code}.csv'
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=True)

                s3_hook.load_string(
                    string_data=csv_buffer.getvalue(),
                    key=key,
                    bucket_name=bucket_name,
                    replace=True
                )
                logging.info(f"Successfully saved data for {company_name} {company_code}")
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
    catchup=False,
) as dag:

    process_and_upload_stock_data_task = PythonOperator(
        task_id='update_kr_stock_data',
        python_callable=update_stock_data,
        dag=dag,
    )

    """
    run_glue_job = AwsGlueJobOperator(
        task_id='kr_stock_data_glue_job',
        job_name='newbstock_kr_stock_data_s3_to_redshift',
        region_name='ap-northeast-2',
        iam_role_name='newbstock_glue_role',
        dag=dag
    )
    """
    
    run_glue_job_task = GlueJobOperator(
        task_id='kr_stock_data_glue_job',
        job_name='newbstock_kr_stock_data_s3_to_redshift',
        script_location='s3://aws-glue-assets-862327261051-ap-northeast-2/scripts/newbstock_kr_stock_datat_s3_to_redshift.py',  # Glue Job에 필요한 스크립트 경로
        region_name='ap-northeast-2',
        iam_role_name='newbstock_glue_role',
        dag=dag
    )
    process_and_upload_stock_data_task >> run_glue_job_task
