import findspark
findspark.init("/home/rajdeep/spark-3.5.0-bin-hadoop3")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.log4j import Log4j
from lib.utils import *


if __name__ == '__main__':

    #getting spark config 
    conf = get_spark_app_config()

    #initializing the spark and logger object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4j(spark)

    # reading the data into df
    df = spark.read.csv("data/invoices.csv", header=True, inferSchema=True)

    # aggregating using object function
    logger.info(df.select(
        count("*").alias("count(*)"),
        sum("Quantity").alias("TotalQuantity"),
        round(avg("UnitPrice"),2).alias("AvgUnitPrice"),
        count_distinct("InvoiceNo").alias("DistinctInvoce")
    ).take(10))

    # aggregating using string function
    logger.info(df.selectExpr(
        "count(1) AS `count(1)`",
        "sum(Quantity) AS TotalQuantity",
        "round(avg(UnitPrice),2) AS AvgUnitPrice",
        "count(distinct(InvoiceNo)) AS DistinctInvoce"
    ).take(10))

    df.printSchema()

    # aggregating using object function
    logger.info(df \
    .groupBy("Country","InvoiceNo") \
    .agg(
        sum("Quantity").alias("TotalQuantity"),
        round(sum(expr("Quantity * UnitPrice")),2).alias("InvoiceValue")
    ).take(10))

    # saving df in temp table
    df.createOrReplaceTempView("invoice")

    # aggregating using spark sql
    logger.info(spark.sql("""
        SELECT 
              Country, 
              InvoiceNo, 
              SUM(Quantity) AS TotalQuantity, 
              ROUND(SUM(Quantity * UnitPrice),2) AS InvoiceValue
        FROM
              invoice
        Group By
            Country, InvoiceNo

""").take(10))

    # aggregating using object function
    logger.info(df \
    .groupBy("Country", weekofyear(to_timestamp("InvoiceDate",'dd-MM-yyyy H.mm')).alias("weekofyear"))\
    .agg(
        count_distinct("InvoiceNo").alias("UniqueInvoice"),
        sum("Quantity").alias("TotalQuantity"),
        round(sum(expr("Quantity * UnitPrice")),2).alias("InvoiceValue")
    ).take(10))

    # stopping the sparks
    spark.stop()