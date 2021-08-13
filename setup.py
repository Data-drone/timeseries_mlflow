### script to assist with testing out mlflow

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd


def start_spark():
    packages="""io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0"""
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-aws:3.2.0 pyspark-shell"

    spark = SparkSession \
                .builder \
                .config("spark.executor.cores", 4) \
                .config("spark.executor.memory", "4g") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.master", "spark://spark-master:7077") \
                .config("spark.hadoop.fs.s3a.access.key", os.environ['MINIO_ACCESS_KEY']) \
                .config("spark.hadoop.fs.s3a.secret.key", os.environ['MINIO_SECRET_KEY']) \
                .config("spark.hadoop.fs.s3a.endpoint", "minio:9000") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.metastore.catalog.default", "hive") \
                .config("spark.sql.warehouse.dir", "s3a://storage/warehouse") \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.connection.maximum", "50") \
                .config("spark.hive.metastore.uris", "thrift://192.168.64.4:9083") \
                .appName("Jupyter Time Series") \
                .enableHiveSupport() \
                .getOrCreate()

    # set to cores to increase the efficiency
    spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism*4)

    ## faster pandas data transfers
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    return spark

from tseries.taxi_daily import TaxiDaily

# credentials for storing our model artifacts
# mlflow needs these to be set whenever it is being called
os.environ['AWS_ACCESS_KEY_ID'] = os.environ.get('MINIO_ACCESS_KEY')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.environ.get('MINIO_SECRET_KEY')
os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'http://minio:9000'


def extract_data(spark):
    """
    
    """

    taxi_daily = TaxiDaily(spark)
    taxi_daily.load_data()

    # MLflow setup
    starting_dataset = taxi_daily.dataset.filter("pickup_date < '2015-09-01'")

    split_date = '2015-08-01'
    train, val = starting_dataset.filter("pickup_date < '{0}'".format(split_date)).toPandas(), \
                    starting_dataset.filter("pickup_date >= '{0}'".format(split_date)).toPandas()

    # set the dates to proper series
    train.index = train['pickup_date']
    train = train.drop(['pickup_date'], axis = 1)
    train.sort_index(inplace=True)
    train = train.asfreq('D')

    val.index = val['pickup_date']
    val = val.drop(['pickup_date'], axis = 1)
    val.sort_index(inplace=True)
    val = val.asfreq('D')

    spark.stop()

    return train, val
