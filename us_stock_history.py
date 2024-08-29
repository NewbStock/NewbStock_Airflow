# airflow library
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta, date
import logging
import time
import FinanceDataReader as fdr
import io
import psycopg2
import pandas as pd

# AWS Redshift 연결
def get_redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_conn')
    conn = hook.get_conn()
    conn.autocommit = True  # default는 False
    return conn.cursor()


def stock_history_upload():
    bucket_name = 'team-won-2-bucket'
    prefix = 'us_stock_data/history/'

    s3_hook = S3Hook(aws_conn_id='s3_conn')

    # 현재 저장된 파일 리스트 추출
    existing_files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    file_list = [file.replace(prefix, '') for file in existing_files if file.endswith('.csv')]
    # 테이블 생성
    cur = get_redshift_connection()
    create_table_query = f"""CREATE TABLE IF NOT EXISTS stock_history.us_stock_data(
        date DATE,
        name VARCHAR(100),
        code VARCHAR(20),
        open_value NUMERIC(8, 2)
    );"""
    cur.execute(create_table_query)


    top100_file = pd.read_csv(io.StringIO(s3_hook.read_key('us_stock_data/us_stock_top100.csv', bucket_name)))
    company_code_list = list(top100_file['code'])
    company_name_list = list(top100_file['name'])

    # 새로운 데이터가 있을 때 glue task로 redshift에 적재
    new_history_list = []

    for i in range(100):
        company_code = company_code_list[i]
        company_name = company_name_list[i]

        file_name = company_code + '.csv'
        save_file_name = prefix + file_name

        try:
            if file_name in file_list:
                recently_date = cur.execute(f"SELECT a.date FROM stock_history.us_stock_data as a WHERE code = '{company_code}' ORDER BY a.date DESC LIMIT 1;")
                cur.fetchall()
                recently_date = str(recently_date[0][0])

                update_hist = fdr.DataReader('YAHOO:' + company_code, recently_date)
                update_hist.index.name = 'Date'
                update_hist.reset_index(inplace=True)
                if len(update_hist) == 1:
                    print(f"{company_name} data is already updated")
                    continue
                update_hist.insert(0, 'Code', company_code)
                update_hist.insert(0, 'Name', company_name)
                update_hist.rename(columns={'Open':'OpenValue'}, inplace=True)

                # 업데이트
                for j in range(len(1, update_hist)):
                    date_var = update_hist.loc[j, 'Date']
                    name_var = update_hist.loc[j, 'Name']
                    code_var = update_hist.loc[j, 'Code']
                    openvalue_var = float(update_hist.loc[j, 'OpenValue'])
                    cur.execute(f"INSERT INTO stock_history.us_stock_data VALUES ({date_var}, {name_var}, {code_var}, {openvalue_var})")
                    logging.info("insert done")
                logging.info(company_name + " file s3 update done")
            else:
                hist = fdr.DataReader('YAHOO:' + company_code)
                hist.index.name = 'Date'
                hist.reset_index(inplace=True)
                hist.insert(0, 'Code', company_code)
                hist.insert(0, 'Name', company_name)
                hist.rename(columns={'Open':'OpenValue'}, inplace=True)
                csv_buffer = io.StringIO()
                hist.to_csv(csv_buffer, index=True)
                s3_hook.load_string(
                    string_data=csv_buffer.getvalue(),
                    key=save_file_name,
                    bucket_name=bucket_name
                )
                new_history_list.append(company_code)
                logging.info(company_name + " file s3 upload done")
        except Exception as ex:
            print('data error', ex)
    if len(new_history_list) != 0:
        new_history_dict = {'code': new_history_list}
        df = pd.DataFrame(new_history_dict)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=True)
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key='us_stock_data/new_history/new_history.csv',
            bucket_name=bucket_name
        )
    cur.close()
default_args = {
    'owner': 'joonghyeon',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 15),
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}
with DAG(
    dag_id='us_stock_etl',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    schedule='0 11 * * 2-6', # 적당히 조절

) as dag:
    history_upload_to_s3_task = PythonOperator (
        task_id='history_upload_to_s3',
        python_callable=stock_history_upload,
        dag=dag
    )

    upload_to_redshift_task = GlueJobOperator(
        task_id='upload_to_redshift',
        job_name='newbstock_us_stock_data_s3_to_redshift',
        script_location='s3://team-won-2-glue-bucket/scripts/newbstock_us_stock_data_s3_to_redshift.py',  # Glue Job에 필요한 스크립트 경로
        region_name='ap-northeast-2',
        iam_role_name='newbstock_glue_role',
        dag=dag
    )

    history_upload_to_s3_task >> upload_to_redshift_task
    