import findspark
findspark.init("/home/rajdeep/spark-3.5.0-bin-hadoop3")

from pyspark.sql import *
from pyspark.sql.functions import *

from lib.log4j import Log4j
from lib.utils import *


if __name__ == '__main__':
    #get spark app config
    conf = get_spark_app_config()

    #initialize spark and logger object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4j(spark)

    #reading the source file
    flight_df = spark.read.parquet("data/flight-time.parquet")

    #checking the number of partition and count of records ineach partition
    logger.info(f"No.of Partitions: {flight_df.rdd.getNumPartitions()}")
    logger.info(flight_df.groupby(spark_partition_id()).count().show())

    #repartitioning the records
    reparitioned_df = flight_df.repartition(5)

    #checking the number of partition and count of records ineach partition after repartitioning
    logger.info(f"No.of Partitions: {reparitioned_df.rdd.getNumPartitions()}")
    logger.info(reparitioned_df.groupby(spark_partition_id()).count().show())

    #writing the data to output 
    #note: to write record in avro format configure spark-avro jar
    #spark.jars.packages org.apache.spark:spark-avro_2.12:3.5.0 like this in spark-default.conf
    reparitioned_df.write\
        .format('avro') \
        .mode('overwrite') \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option('maxRecordsPerFile',10000) \
        .save('output_data/avro/')


    spark.stop()
