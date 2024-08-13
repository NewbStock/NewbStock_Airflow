"""
S3에서 'kr_top100.csv' 데이터 가져오기
만약 {CompanyCode}.csv 파일이 S3에 없다면, FinanceDataReader에서 2000년 데이터부터 다 가져오기
파일이 이미 있다면, 당일 날짜의 데이터만 가져와서 원래 파일에 추가
"""
# pip install finance-datareader
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import logging
from io import StringIO, BytesIO
import pyarrow as pa
import pyarrow.parquet as pq


# S3에서 'kr_top100.csv' (오늘 시가총액 top100) 데이터 가져오기
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
        return None


# S3에 시가총액 Top100 데이터 업데이트 및 생성
def update_stock_data():
    bucket_name = 'team-won-2-bucket'
    s3_hook = S3Hook(aws_conn_id='s3_conn') 

    # 한국 시장 시가총액 100위 데이터 가져오기
    kr_top100 = get_kr_top100()
    if kr_top100 is None:
        logging.error("Failed to get kr_top100 data. Aborting.")
        return
    
  

    # 현재 top 100 기업의 데이터 업데이트 또는 새로 생성
    for _, company in kr_top100.iterrows():
        company_name = company['name']
        company_code = str(company['code']).zfill(6)
        key = f'kr_stock_data/parquet/{company_code}.csv'
        #company_code = str(company['code']).zfill(6)  # 'code' 컬럼의 값을 6자리로 맞추기 (앞에 0 추가)

        try:
            df = fdr.DataReader(f'KRX:{company_code}', start = '2000-01-01')        
            df.index.name = 'Date'  # 인덱스 이름 설정
            df.reset_index(inplace=True)  # 인덱스를 컬럼으로 변환

            df['name'] = company_name
            df['code'] = company_code

            # FinanceDataReader 컬럼
            # Date, Open, High, Low, Close, Volume, Change, Updown, Comp, Amount, MarCap, Shares
            if not df.empty:
                # Convert DataFrame to PyArrow Table
                table = pa.Table.from_pandas(df)

                # Write to Parquet format
                parquet_buffer = BytesIO()
                pq.write_table(table, parquet_buffer)

                s3_hook.load_bytes(
                    bytes_data=parquet_buffer.getvalue(),
                    key=key,
                    bucket_name=bucket_name,
                    replace=True
                )
                logging.info(f"Successfully saved Parquet data for {company_name} {company_code}")


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
    dag_id='get_kr_stock_data_test',
    default_args=default_args,
    description='Save file as Parquet, Avro, OCR',
    schedule_interval='30 9 * * 1-5',  # UTC 09:30 (KST 18:30), 월요일부터 금요일까지 장 마감 후  
    start_date=datetime(2024, 7, 15),  # 시작 날짜
    catchup=False,
) as dag:

    process_and_upload_stock_data_task = PythonOperator(
        task_id='save_file',
        python_callable=update_stock_data,
        dag=dag,
    )
