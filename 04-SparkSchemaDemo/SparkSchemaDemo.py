import findspark
findspark.init("/home/rajdeep/spark-3.5.0-bin-hadoop3")

from pyspark.sql import *
from pyspark.sql.types import *

from lib.utils import *
from lib.log4j import Lo4j

if __name__ == '__main__':
    #get the spark app config
    conf = get_spark_app_config()

    #create the spark object and logger object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Lo4j(spark)


    #define schema programmatically
    schema = StructType(
        [StructField("FL_DATE",DateType()),
        StructField("OP_CARRIER",StringType()),
        StructField("OP_CARRIER_FL_NUM",IntegerType()),
        StructField("ORIGIN",StringType()),
        StructField("ORIGIN_CITY_NAME",StringType()),
        StructField("DEST",StringType()),
        StructField("DEST_CITY_NAME",StringType()),
        StructField("CRS_DEP_TIME",IntegerType()),
        StructField("DEP_TIME",IntegerType()),
        StructField("WHEELS_ON",IntegerType()),
        StructField("TAXI_IN",IntegerType()),
        StructField("CRS_ARR_TIME",IntegerType()),
        StructField("ARR_TIME",IntegerType()),
        StructField("CANCELLED",IntegerType()),
        StructField("DISTANCE",IntegerType())]
    )

    #read csv data with specified schema
    flightTimeCSV_df = spark.read.csv("data/flight-time.csv",header=True,schema=schema, mode = 'FAILFAST',dateFormat = 'M/d/y')

    #printing the schema of csv data
    # logger.info(flightTimeCSV_df.show())
    logger.info("CSV DF Schema")
    logger.info(flightTimeCSV_df.printSchema())

    #define schema using DDL
    schema_ddl = 'FL_DATE DATE,OP_CARRIER STRING,OP_CARRIER_FL_NUM STRING,ORIGIN STRING,ORIGIN_CITY_NAME STRING, \
    DEST STRING,DEST_CITY_NAME STRING,CRS_DEP_TIME INT,DEP_TIME INT,WHEELS_ON INT,TAXI_IN INT,CRS_ARR_TIME INT, \
    ARR_TIME INT,CANCELLED INT,DISTANCE INT'

    #read json data with specified schema
    flightTimeJSON_df = spark.read.json("data/flight-time.json",schema=schema_ddl,mode='FAILFAST',dateFormat='M/d/y')

    #printing the schema of csv data
    # logger.info(flightTimeJSON_df.show())
    logger.info("JSON DF Schema")
    logger.info(flightTimeJSON_df.printSchema())


    #read parquet data without specified schema as parquet data have schema specified in file itself
    flightTimeParquet_df = spark.read.parquet("data/flight-time.parquet")
    # logger.info(flightTimeParquet_df.show())

    #printing the schema of csv datas
    logger.info("Parquet DF Schema")
    logger.info(flightTimeParquet_df.printSchema())


    spark.stop()