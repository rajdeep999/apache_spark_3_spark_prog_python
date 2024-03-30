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

    # df1.show()
    # df2.show()

    # join condition
    join_expr = flight_df1.id == flight_df2.id
    # joining the df
    join_df = flight_df1.join(flight_df2, join_expr, "inner")

    join_df.collect()

    # input()

    spark.stop()