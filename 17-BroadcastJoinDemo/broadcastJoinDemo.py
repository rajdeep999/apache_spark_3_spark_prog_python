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
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4j(spark)

    # reading the data in df's
    flight_df1 = spark.read.json("data/d1/")
    flight_df2 = spark.read.json("data/d2/")


    # print(flight_df1.count())
    # print(flight_df2.count())

    # join condition
    join_expr = flight_df1.id == flight_df2.id
    # joining the df using broadcast join
    # when right side df is small in size then that can be send to larger df so as to avoid shuffle and faster join
    join_df = flight_df1.join(broadcast(flight_df2), join_expr, "inner")

    join_df.collect()

    # input()


    # issues with join and some possible way to resolve it
    # huge volume of data: filter/aggregate before joining
    # parallelism - shuffles/ executors/ keys
    #   # maximum possible parellelism that can be achieved with shuffle is:
    #   # min(max_no_of_executors, max_no_of_partition, max_no_og_unique_key)
    # shuffle distributiors(key skews) - bucketing

    # stopping the spark
    spark.stop()