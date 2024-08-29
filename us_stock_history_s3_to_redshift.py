# glue task

import sys
import boto3
import pandas as pd
import io

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue import DynamicFrame
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


# AWS 서비스 클라이언트 설정
s3_client = boto3.client('s3')

# Redshift 설정
redshift_temp_dir = "s3://team-won-2-bucket/temp/"
redshift_table_name = "us_stock_data"
redshift_schema_name = "stock_history"
redshift_connection_name = "newbstock_redshift_connection"
redshift_database_name = "dev"

# S3 버킷, 경로
bucket_name = 'team-won-2-bucket'
s3_path = 'us_stock_data/history/'

# 적재할 데이터가 있는 지 확인
def data_to_redshift():
    try:
        new_data_hist = pd.read_csv(
            io.BytesIO(
                s3_client.get_object(
                    Bucket=bucket_name,
                    Key="us_stock_data/new_history/new_history.csv"
                )['Body'].read()
            )
        )
        new_data_list = list(new_data_hist['code'])
        response = s3_client.delete_object(
            Bucket=bucket_name,
            Key="us_stock_data/new_history/new_history.csv"
        )
    except Exception as ex:
        print('data empty', ex)
        return
    
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    for i in range(len(new_data_list)):
    
        # 새로 들어온 파일 적재
        file_key = s3_path + new_data_list[i] + ".csv"
    
        file_path = f"s3://{bucket_name}/{file_key}"
        df = spark.read.format("csv").option("header", "true").load(file_path)
        if df.count() == 0:
            print("no data")
            continue
    
        df = df.withColumn("date", df["Date"].cast("date")) \
                .withColumn("name", df["Name"]) \
                .withColumn("code", df["Code"]) \
                .withColumn("open_value", df["OpenValue"].cast("decimal(8,2)")) \
                .select('date', 'name', 'code', 'open_value')
                
        dynamic_frame = DynamicFrame.fromDF(df, glue_context, "dynamic_frame")

        glue_context.write_dynamic_frame.from_jdbc_conf(
            frame = dynamic_frame,
            catalog_connection = redshift_connection_name, 
            connection_options = {
                "dbtable": redshift_schema_name + "." + redshift_table_name,
                "database": redshift_database_name
            }, 
            redshift_tmp_dir = redshift_temp_dir
        )

    job.commit()

data_to_redshift()
