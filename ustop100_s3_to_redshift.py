import psycopg2

# Redshift 연결
conn = psycopg2.connect(
    host="team-won-2-redshift-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com",
    database="dev",
    user="newbstock_admin",
    password="Newbstock001!",
    port=5439,
)
cur = conn.cursor()

s3_path = 's3://team-won-2-bucket/us_stock_data/us_stock_top100.csv'
iam_role = 'arn:aws:iam::862327261051:role/service-role/AmazonRedshift-CommandsAccessRole-20240716T180249'

try:
    cur.execute("BEGIN;")
    cur.execute(f"""DROP TABLE IF EXISTS ustop100;""")
    create_ustop100_table_query = """CREATE TABLE ustop100(
        ranking INT,
        name VARCHAR(100),
        code VARCHAR(20),
        excode VARCHAR(20)
    );"""
    cur.execute(create_ustop100_table_query)
    
    copy_sql = f"""
        COPY ustop100
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        CSV
        IGNOREHEADER 1;
    """
    cur.execute(copy_sql)
    cur.execute("COMMIT;")
except Exception as e:
    cur.execute("ROLLBACK;")
    raise e

cur.close()
conn.close()
