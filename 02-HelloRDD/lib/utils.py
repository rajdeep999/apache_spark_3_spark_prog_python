from pyspark import SparkConf

import configparser


def get_spark_app_config():
    config = configparser.ConfigParser()
    config.read("spark.conf")

    spark_conf = SparkConf()
    for (key, val) in config.items('SPARK_APP_CONFIG'):
        spark_conf.set(key,val)

    return spark_conf