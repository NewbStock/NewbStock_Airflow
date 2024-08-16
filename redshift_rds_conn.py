import boto3
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import os
import gzip
import io

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
    dag_id='redshift_to_s3_and_rds_optimized',
    default_args=default_args,
    description='Optimized: Extract all tables from Redshift public schema, upload to S3, and load to RDS daily',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def redshift_to_s3_and_rds():

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
        주어진 테이블의 데이터를 추출하여 압축 후 S3에 업로드합니다.
        """
        redshift_conn_id = 'redshift_conn'
        redshift_hook = PostgresHook(postgres_conn_id=redshift_conn_id)
        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_bucket = 'team-won-2-redshift-rds-conn'
        s3_key = f"redshift_data/{table_name}.csv.gz"

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

            # 데이터를 압축하여 메모리 버퍼에 저장
            with io.BytesIO() as mem_file:
                with gzip.GzipFile(fileobj=mem_file, mode='wb') as gz:
                    gz.write(('\t'.join(columns) + '\n').encode('utf-8'))  # 탭으로 구분
                    for row in data:
                        gz.write(('\t'.join(map(str, row)) + '\n').encode('utf-8'))  # 탭으로 구분
                mem_file.seek(0)
                s3_hook.load_file_obj(mem_file, key=s3_key, bucket_name=s3_bucket, replace=True)


            logging.info(f"File uploaded to S3: s3://{s3_bucket}/{s3_key}")

        except Exception as e:
            logging.error(f"Error processing table {table_name}: {e}")
            raise

        return s3_key

    @task(task_id="load_to_rds")
    def load_to_rds(s3_key):
        """
        S3에서 PostgreSQL RDS로 데이터를 로드합니다.
        """
        rds_conn_id = 'rds_conn'
        s3_bucket = 'team-won-2-redshift-rds-conn'
        local_file_path = f"/tmp/{s3_key.split('/')[-1]}"

        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_hook.get_conn().download_file(s3_bucket, s3_key, local_file_path)

        rds_hook = PostgresHook(postgres_conn_id=rds_conn_id)
        conn = rds_hook.get_conn()
        cursor = conn.cursor()

        try:
            with gzip.open(local_file_path, 'rt', encoding='utf-8') as f:
                cursor.copy_expert(f"COPY public.{s3_key.split('/')[-1].replace('.csv.gz', '')} FROM STDIN WITH CSV HEADER;", f)
            
            conn.commit()
            logging.info(f"Data from {s3_key} loaded to RDS successfully.")
        except Exception as e:
            logging.error(f"Error loading data to RDS from {s3_key}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
            if os.path.exists(local_file_path):
                os.remove(local_file_path)

    # DAG의 태스크들 연결
    table_names = get_public_tables()
    s3_keys = extract_and_upload_table.expand(table_name=table_names)
    load_to_rds.expand(s3_key=s3_keys)

# DAG 인스턴스 생성
dag = redshift_to_s3_and_rds()
