import findspark
findspark.init("/home/rajdeep/spark-3.5.0-bin-hadoop3")


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from lib.utils import *
from lib.log4j import Log4j


if __name__ == '__main__':
    # getting the spark config
    conf = get_spark_app_config()

    # initialing the spark and logger object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4j(spark)

    # order data in list of tuple form
    orders_list = [("01", "02", 350, 1),
                   ("01", "04", 580, 1),
                   ("01", "07", 320, 2),
                   ("02", "03", 450, 1),
                   ("02", "06", 220, 1),
                   ("03", "01", 195, 1),
                   ("04", "09", 270, 3),
                   ("04", "08", 410, 2),
                   ("05", "02", 350, 1)]

    # initializing the order df
    order_df = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")

    # product data in list of tuple form
    product_list = [("01", "Scroll Mouse", 250, 20),
                    ("02", "Optical Mouse", 350, 20),
                    ("03", "Wireless Mouse", 450, 50),
                    ("04", "Wireless Keyboard", 580, 50),
                    ("05", "Standard Keyboard", 360, 10),
                    ("06", "16 GB Flash Storage", 240, 100),
                    ("07", "32 GB Flash Storage", 320, 50),
                    ("08", "64 GB Flash Storage", 430, 25)]

    # initializing the product df
    product_df = spark.createDataFrame(product_list).toDF("prod_id", "prod_name", "list_price", "qty")

    # renaming a same column name so as to avoid error while selecting same name col
    renamed_df = product_df.withColumnRenamed("qty","product_qty")

    # join condition
    join_expr = order_df.prod_id == renamed_df.prod_id
    # joining the df
    logger.info(order_df.join(renamed_df, join_expr, "left") \
            .drop(renamed_df.prod_id) \
            .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty") \
            .withColumn("prod_name", coalesce("prod_name","prod_id")) \
            .withColumn("list_price", coalesce("list_price", "unit_price")) \
            .sort("order_id") \
            .collect())

    # stopping the spark
    spark.stop()