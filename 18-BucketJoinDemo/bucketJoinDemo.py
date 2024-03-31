import findspark
findspark.init("/home/rajdeep/spark-3.5.0-bin-hadoop3")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from lib.log4j import Log4j
from lib.utils import *


if __name__ == '__main__':
    # getting the spark config
    conf = get_spark_app_config()

    # initialing the spark and logger object
    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    logger = Log4j(spark)

    # reading the data in df's
    # flight_df1 = spark.read.json("data/d1/")
    # flight_df2 = spark.read.json("data/d2/")

    # creating db in hive to store data
    # spark.sql("CREATE DATABASE IF NOT EXISTS my_db")

    # storing data in hive
    # flight_df1.coalesce(1).write.bucketBy(3, 'id').mode("overwrite").saveAsTable("my_db.flight_df1")
    # flight_df2.coalesce(1).write.bucketBy(3, 'id').mode("overwrite").saveAsTable("my_db.flight_df2")

    # using the created db
    spark.sql("USE my_db")

    # reading the tables
    df1 = spark.sql("SELECT * FROM flight_df1")
    df2 = spark.sql("SELECT * FROM flight_df2")

    # join expression
    join_expr = df1.id == df2.id
    # joining the tables
    join_df = df1.join(df2, join_expr, 'inner')

    join_df.collect()
    # input()

    # stopping the spark
    spark.stop()