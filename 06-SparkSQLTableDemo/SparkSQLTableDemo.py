import findspark
findspark.init("/home/rajdeep/spark-3.5.0-bin-hadoop3")

from pyspark.sql import *
from lib.log4j import Log4j
from lib.utils import *



if __name__ == '__main__':
    #get the spark app config
    conf = get_spark_app_config()

    #create the spark object and logger object
    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    logger = Log4j(spark)


    #read the file from the mentioned path
    file_path = "data/flight-time.parquet"
    flightTime_df = read_src_data(spark,file_path)

    #create a db in sql hive and using the created db
    spark.sql("CREATE DATABASE IF NOT EXISTS flight_dataset")
    spark.catalog.setCurrentDatabase("flight_dataset")

    #writing the df to the hive db
    flightTime_df.write \
                .format("csv") \
                .mode("overwrite") \
                .bucketBy(10,"OP_CARRIER","ORIGIN") \
                .saveAsTable("flightTime")

    #listing the tables present in the db
    logger.info(spark.catalog.listTables("flight_dataset"))


    # stopping the spark
    spark.stop()