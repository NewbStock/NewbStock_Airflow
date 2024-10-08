from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime
from plugins import slack

default_args = {
    'owner': 'kyoungyeon',
    'depends_on_past': False,
    'retries': 1,
    'on_failure_callback': slack.on_failure_callback,
}

with DAG ( 
    dag_id='trigger_glue_job',
    default_args=default_args,
    description='Trigger Glue Job newbstock_kr_stock_data_s3_to_redshift ',
    schedule_interval=None,
    start_date=datetime(2024, 7, 15),  # 시작 날짜
    catchup=False,
) as dag:
    
    run_glue_job_task = GlueJobOperator(
        task_id='kr_stock_data_glue_job',
        job_name='newbstock_kr_stock_data_s3_to_redshift',
        script_location='s3://aws-glue-assets-862327261051-ap-northeast-2/scripts/newbstock_kr_stock_data_s3_to_redshift.py',  # Glue Job에 필요한 스크립트 경로
        region_name='ap-northeast-2',
        iam_role_name='newbstock_glue_role',
        dag=dag
    )

    run_glue_job_task
