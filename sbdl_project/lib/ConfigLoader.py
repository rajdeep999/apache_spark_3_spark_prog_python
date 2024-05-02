import configparser
from pyspark import SparkConf

#read the app config
def get_config(env):
    conf = {}
    config = configparser.ConfigParser()
    config.read("conf/sbdl.conf")

    for key, val in config.items(env):
        conf[key] = val
    return conf

#read the spark config
def get_spark_config(env):
    spark_conf = SparkConf()

    config = configparser.ConfigParser()
    config.read("conf/spark.conf")

    for key, val in config.items(env):
        spark_conf.set(key, val)

    return spark_conf

#check if any runtime filter need to be applied while reading data
def get_filter(env, data_filter):
    conf = get_config(env)
    return "true" if  conf[data_filter] == "" else conf[data_filter]