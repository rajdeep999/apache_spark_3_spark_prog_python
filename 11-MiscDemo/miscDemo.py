import findspark
findspark.init('/home/rajdeep/spark-3.5.0-bin-hadoop3')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.utils import *
from lib.log4j import Log4j


if __name__ == '__main__':
    # fetching spark app config
    conf = get_spark_app_config()

    #initializing the spark and logger object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4j(spark)


    #creating list of tuples
    data_list = [("Ravi", "28", "1", "2002"),
                 ("Abdul", "23", "5", "81"),  # 1981
                 ("John", "12", "12", "6"),  # 2006
                 ("Rosy", "7", "8", "63"),  # 1963
                 ("Abdul", "23", "5", "81")]  # 1981
    
    #creating the df using list of tuples
    raw_df = spark.createDataFrame(data_list).toDF("name","day","month","year").repartition(5)


    # some micellaneous functions:
    # add monotonically_increasing_id
    # cast datatype
    # when otherwise condition
    # adding columns based on existing columns
    # dropping columns
    # dropping duplicates
    # sorting df
    final_df = raw_df \
                    .withColumn("id",monotonically_increasing_id())  \
                    .withColumn("day", col("day").cast(IntegerType())) \
                    .withColumn("month", col("month").cast(IntegerType())) \
                    .withColumn("year", col("year").cast(IntegerType())) \
                    .withColumn("year", (
                        when(col("year")<24 , col("year")+2000) \
                        .when(col("year")<100, col("year")+1900) \
                        .otherwise(col("year"))
                    )) \
                    .withColumn("dob", to_date(concat(col("day"),lit("/"),col("month"),lit("/"),col("year")),'d/M/y')) \
                    .drop("day","month","year") \
                    .dropDuplicates(["name","dob"]) \
                    .sort("dob", ascending=False)

    logger.info(final_df.collect())


    # stopping spark object
    spark.stop()