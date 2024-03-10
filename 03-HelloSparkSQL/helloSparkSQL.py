import findspark
findspark.init("/home/rajdeep/spark-3.5.0-bin-hadoop3")

from pyspark.sql import *
from lib.log4j import Log4j
from lib.utils import *

if __name__ == '__main__':

    #get spark config
    conf = get_spark_app_config()

    #initalize spark object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    #initializing logger object
    logger = Log4j(spark)
    logger.info("Spark Started")

    #reading the data from csv file
    df = spark.read.csv("data/sample.csv", header=True, inferSchema=True)
    #creating a temp view
    df.createTempView('survey')


    #running the sql query on dataframe using SparkSQL
    country_count = spark.sql("SELECT country, count(*) FROM survey WHERE age < 40 GROUP BY country").collect()

    logger.info(country_count)

    #stopping spark object
    logger.info("Spark Stopped")
    spark.stop()