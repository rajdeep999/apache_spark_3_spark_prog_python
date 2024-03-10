import findspark
findspark.init("/home/rajdeep/spark-3.5.0-bin-hadoop3")

from pyspark.sql import *
from lib.utils import *
from lib.log4j import Log4J
from collections import namedtuple


if __name__ == '__main__':
    conf = get_spark_app_config()
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc =spark.sparkContext
    
    logger = Log4J(spark)

    logger.info("Spark start")


    surveyRecords =namedtuple('surveyRecords',['age','gender','country','state'])
    rdd = sc.textFile("data/sample.csv")
    selectRDD = rdd.map(lambda row: row.replace('"','').split(","))
    structRDD = selectRDD.map(lambda row: surveyRecords(int(row[1]),row[2],row[3],row[4]))
    filterRDD = structRDD.filter(lambda row: row.age<40)
    mapRDD = filterRDD.map(lambda row: (row.country,1))
    reduceRDD = mapRDD.reduceByKey(lambda v1, v2: v1+v2).collect()

    logger.info(reduceRDD)
    
    print(reduceRDD)

    logger.info("Spark stop")
    spark.stop()
