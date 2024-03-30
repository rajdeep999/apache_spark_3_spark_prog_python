from pyspark import SparkConf

import configparser

def get_spark_app_config():
    conf = SparkConf()

    config = configparser.ConfigParser()
    config.read("spark.conf")

    for key, val in config.items("SPARK_APP_CONFIG"):
        conf.set(key, val)

    return conf
