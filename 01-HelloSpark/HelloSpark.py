import findspark
findspark.init("/home/rajdeep/spark-3.5.0-bin-hadoop3")

from pyspark.sql import *
from lib.log4j import Log4j
from lib.utils import *
import sys


if __name__ == '__main__':
    #get spark config
    sparkconf = get_spark_conf_config()
    #initialize spark 
    spark = SparkSession.builder.config(conf=sparkconf).getOrCreate()

    # setup logger
    logger = Log4j(spark)
    logger.info("spark started")

    # To check spark config
    # spark_conf = spark.sparkContext.getConf() 
    # logger.info(spark_conf.toDebugString())

    #check if file path is provided
    if len(sys.argv) != 2:
        logger.error("File name not passed")
        sys.exit(-1)
        

    # load the df
    df = load_survey_df(spark,sys.argv[1])

    #repartition the df
    df = df.repartition(2)

    #filter df based on age then group by country count after selecting required columns data
    filtered_df = count_by_country(df)
    
    # collecting the country count
    logger.info(filtered_df.collect())

    logger.info("spark stopped")
    spark.stop()