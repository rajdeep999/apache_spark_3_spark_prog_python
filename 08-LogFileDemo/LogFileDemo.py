import findspark
findspark.init("/home/rajdeep/spark-3.5.0-bin-hadoop3")

from pyspark.sql import *
from pyspark.sql.functions import *

from libs.utils import *
from libs.log4j import Log4j


if __name__ == '__main__':
    #get spark app config
    conf = get_spark_app_config()

    #configure and initialize spark and logger
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4j(spark)

    #regex to extract columns from df
    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
    #reading log text file
    df = spark.read.text('data/apache_logs.txt')

    #extracting required coolumns
    df = df.select(regexp_extract('value',log_reg,1).alias('ip'),
                   regexp_extract('value',log_reg,4).alias('date'),
                   regexp_extract('value',log_reg,6).alias('request'),
                   regexp_extract('value',log_reg,10).alias('referral')
                )
    

    #grouping by referral and ordering it by count desc
    logger.info(df \
                .where('referral != "-"') \
                .withColumn('referral',substring_index('referral','/',3)) \
                .groupBy('referral') \
                .count() \
                .orderBy('count', ascending=False) \
                .collect()
    )


    #stopping the spark object
    spark.stop()