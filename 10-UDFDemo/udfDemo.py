import findspark

findspark.init('/home/rajdeep/spark-3.5.0-bin-hadoop3')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.utils import *
from lib.log4j import Log4j
import re

#defined the udf function
def parse_gender(gender):
    female_pattern = r'^f$|f.m|w.m'
    male_pattern = r'@m$|ma|m.l'

    if re.search(female_pattern, gender.lower()):
        return 'Female'
    elif re.search(male_pattern, gender.lower()):
        return 'Male'
    else:
        return 'unknown'


if __name__ == '__main__':
    #fetch the spark config
    conf = get_spark_app_config()

    #initialize the spark and logger object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4j(spark)

    #read the dataframe
    df = spark.read.csv("data/survey.csv", header=True, inferSchema=True)

    #logging the current gender value count
    logger.info(df.groupBy('Gender').count().show())



    #registering the udf with spark session
    parseGender = udf(parse_gender, returnType=StringType())
    #checking if udf is register in sql.catalog, it shouldnt as its register with sparksession
    logger.info("Spark Catalog:")
    logger.info(spark.catalog.functionExists('parseGender'))
    #appling the udf
    df = df.withColumn("Gender", parseGender('Gender'))
    #logging new gender value count
    logger.info(df.groupBy('Gender').count().show())




    #registering udf with spark catalog to use with spark sql
    parseGender = spark.udf.register('parseGender', parse_gender, StringType())
    #checking if udf is register in sql.catalog, it should be
    logger.info("Spark Catalog:" )
    logger.info(spark.catalog.functionExists('parseGender'))
    #applying the udf
    df = df.withColumn("Gender", expr("parseGender(Gender)"))
    #logging new gender value count
    logger.info(df.groupBy('Gender').count().show())


    #stopping the spark object
    spark.stop()
