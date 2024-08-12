# airflow library
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta, date
import logging
import time


default_args = {
    'owner': 'joonghyeon',
    'depends_in_past': False,
    'start_date': datetime(2024, 7, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}
with DAG(
    dag_id='delete_history_dag',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    schedule='@weekly', # 적당히 조절
) as dag:
    delete_task = GlueJobOperator(
        task_id='delete_data',
        job_name='newbstock_delete_history',
        script_location='s3://team-won-2-glue-bucket/scripts/newbstock_delete_history.py',  # Glue Job에 필요한 스크립트 경로
        region_name='ap-northeast-2',
        iam_role_name='newbstock_glue_role',
        dag=dag
    )

    delete_task
