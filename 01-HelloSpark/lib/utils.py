import configparser
from pyspark import SparkConf

def get_spark_conf_config():
    sparkConf = SparkConf()

    config = configparser.ConfigParser()
    config.read('spark.conf')

    for (key, val) in config.items("SPARK_APP_CONFIG"):
        sparkConf.set(key,val)

    return sparkConf


def load_survey_df(spark,file_path):
    df = spark.read.csv(file_path,header=True,inferSchema=True)
    return df


def count_by_country(df):
    return df.filter("age < 40") \
                    .select("age","gender","country","state") \
                    .groupby("country") \
                    .count()
    
     