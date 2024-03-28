import findspark
findspark.init('/home/rajdeep/spark-3.5.0-bin-hadoop3')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from lib.utils import *
from lib.log4j import Log4j


if __name__ == '__main__':
    #getting the spark config from spark.conf
    conf = get_spark_app_config()

    #initializing the spark and logge object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4j(spark)

    #reading the data
    df = spark.read.csv('data/flights.csv', inferSchema=True, samplingRatio=0.0001, header=True)

    #selecting req columns using string expression
    logger.info(df.select('Origin','Dest','Distance').take(10))

    #selecting req columns using column object expression
    logger.info(df.select(col('Origin'),col('Dest'),col('Distance')).take(10))

    #creating daate column using string expression
    logger.info(df.select("Origin", "Dest", "Distance", 
                          expr("to_date(concat(Year,Month,lpad(DayofMonth,2,'0')),'yyyyMMdd') as FlightDate")).take(10))

    #creating daate column using column object expression
    logger.info(df.select(col("Origin"), col("Dest"),col("Distance"),
                          to_date(concat(col("year"), col("Month"),lpad(col("DayofMonth"),2,"0")),"yyyyMMdd").alias("FlightDate")).take(10))

    #stopping the spark object
    spark.stop()