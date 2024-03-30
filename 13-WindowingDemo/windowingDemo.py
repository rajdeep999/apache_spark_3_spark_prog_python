import findspark
findspark.init("/home/rajdeep/spark-3.5.0-bin-hadoop3")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from lib.log4j import Log4j
from lib.utils import *


if __name__ == '__main__':
    #getting the spark config
    conf = get_spark_app_config()

    #initializing the spark and logger object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4j(spark)

    # reading data into spark dataframe
    df = spark.read.csv("data/invoices.csv", header=True, inferSchema=True)

    # defining the groupby columns
    numInvoice = count_distinct("InvoiceNo").alias("numInvoice")
    totalQuantity = sum("Quantity").alias("totalQuantity")
    invoiceValue = round(sum(expr("Quantity * UnitPrice")),2).alias("invoiceValue")

    # grouping the data
    summary_df = df \
    .groupBy("Country", weekofyear(to_timestamp("InvoiceDate",'dd-MM-yyyy H.mm')).alias("weekofyear")) \
    .agg(numInvoice, totalQuantity, invoiceValue)

    # defining the window
    running_total_window = Window \
                            .partitionBy("Country") \
                            .orderBy("weekofyear") \
                            .rangeBetween(Window.unboundedPreceding, Window.currentRow)

    # applying the running total window on invoiceValue data
    logger.info(summary_df \
        .withColumn("RunningTotal", round(sum("invoiceValue").over(running_total_window),2)) \
        .take(50))

    # stopping the spark object
    spark.stop()