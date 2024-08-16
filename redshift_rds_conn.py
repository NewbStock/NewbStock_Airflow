import boto3
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import os

# 기본 DAG 인자 설정
default_args = {
    'owner': 'seungjun',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
@dag(
    dag_id='redshift_to_s3_all_public_tables',
    default_args=default_args,
    description='Extract all tables from Redshift public schema and upload to S3 daily',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def redshift_to_s3_all_public_tables():

    @task(task_id="get_public_tables")
    def get_public_tables():
        """
        Redshift의 public 스키마에 있는 모든 테이블 이름을 가져옵니다.
        """
        redshift_conn_id = 'redshift_conn'
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)

        get_tables_query = """
            SELECT DISTINCT tablename
            FROM pg_table_def
            WHERE schemaname = 'public';
        """

        logging.info("Fetching list of tables in public schema...")
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(get_tables_query)
        tables = cursor.fetchall()
        cursor.close()
        conn.close()

        table_names = [table[0] for table in tables]
        logging.info(f"Tables found: {table_names}")
        return table_names

    @task(task_id="extract_and_upload_table")
    def extract_and_upload_table(table_name):
        """
        주어진 테이블의 데이터를 추출하여 S3에 업로드합니다.
        """
        redshift_conn_id = 'redshift_conn'
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-redshift-rds-conn'
        local_file_path = f"/tmp/{table_name}.csv"
        s3_key = f"redshift_data/{table_name}.csv"

        extract_query = f"""
            SELECT *
            FROM public.{table_name}
        """

        try:
            logging.info(f"Extracting data from {table_name}...")
            conn = redshift_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(extract_query)
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            with open(local_file_path, 'w') as f:
                f.write(','.join(columns) + '\n')
                for row in data:
                    f.write(','.join(map(str, row)) + '\n')

            cursor.close()
            conn.close()

            logging.info(f"Uploading {local_file_path} to S3 bucket {s3_bucket} with key {s3_key}...")
            s3_hook.load_file(filename=local_file_path, key=s3_key, bucket_name=s3_bucket, replace=True)
            logging.info(f"File uploaded to S3: s3://{s3_bucket}/{s3_key}")

            os.remove(local_file_path)
            logging.info(f"Local file {local_file_path} deleted.")
        except Exception as e:
            logging.error(f"Error processing table {table_name}: {e}")
            raise

    # DAG의 태스크들 연결
    table_names = get_public_tables()
    extract_and_upload_table.expand(table_name=table_names)

# DAG 인스턴스 생성
dag = redshift_to_s3_all_public_tables()
