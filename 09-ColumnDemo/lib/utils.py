from pyspark import SparkConf

import configparser

def get_spark_app_config():
    config = configparser.ConfigParser()
    config.read('spark.conf')

    conf = SparkConf()

    for key, val in config.items('SPARK_APP_CONFIG'):
        conf.set(key, val)

    return conf