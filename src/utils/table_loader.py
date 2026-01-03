from utils.path_loader import load_paths
from pyspark.sql import *
from utils.spark_session import SparkSessionStart

from utils.env_variables import (
    SOURCE_HOST, SOURCE_PORT,
    SOURCE_USERNAME, SOURCE_PASSWORD, SOURCE_DATABASE,
    TARGET_HOST, TARGET_PORT,
    TARGET_USERNAME, TARGET_PASSWORD,
)

def read_jdbc(spark, schema, table_name, mode, start_time):
    jdbc_url = f"jdbc:postgresql://{SOURCE_HOST}:{SOURCE_PORT}/{SOURCE_DATABASE}"
    properties = {
            "user": SOURCE_USERNAME,
            "password": SOURCE_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
    
    bounds_query = f"""
        SELECT MIN(id) AS lower_bound, MAX(id) AS upper_bound, MAX(updated_at) AS batch_cutoff
        FROM (
        SELECT id, updated_at
        FROM {schema}.{table_name}
        WHERE updated_at > '{start_time}'
        ORDER BY updated_at ASC
        LIMIT 10000000
        ) t
        """
    
    bounds = (
        spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("query", bounds_query)
            .options(**properties)
            .load()
            .first()
    )

    lower_bound = bounds["lower_bound"]
    upper_bound = bounds["upper_bound"]
    batch_cutoff = bounds["batch_cutoff"]

    if batch_cutoff is None:
        return 0, None
    
    query = f"""
                SELECT * 
                FROM {schema}.{table_name}
            """
    if mode == 'incremental':
        query += f"""
            WHERE updated_at > '{start_time}' AND updated_at <= '{batch_cutoff}'
            """
    elif mode == 'backfill':
        query += f"""
            WHERE updated_at >= '{start_time}' AND updated_at <= '{batch_cutoff}'
            """

    df = (spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"({query}) AS subq")
            .option("partitionColumn", "id")
            .option("lowerBound", lower_bound)
            .option("upperBound", upper_bound)
            .option("numPartitions", 20)
            .option("fetchsize", 10000)
            .options(**properties)
            .load())
    return df, batch_cutoff

def read_landing_parquet(spark, schema, table, partition_key=None, partition_value=None):

    LANDING_PATH = load_paths('landing')
    
    if partition_key:
        df = spark.read.parquet(f'{LANDING_PATH}/{schema}.{table}/{partition_key}={partition_value}')
    else:
        df = spark.read.parquet(f'{LANDING_PATH}/{schema}.{table}')
    return df

def read_clickhouse(spark, schema, table):
    df = spark.read.format("jdbc") \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .option("url", f"jdbc:clickhouse://{TARGET_HOST}:{TARGET_PORT}/{schema}") \
                .option("dbtable", table) \
                .option("user", TARGET_USERNAME) \
                .option("password", TARGET_PASSWORD) \
                .load()
    return df

if __name__ == '__main__':
    spark = SparkSessionStart.get_spark()
    df = read_jdbc(spark, 'chief_4.product')
    print(df.count())
