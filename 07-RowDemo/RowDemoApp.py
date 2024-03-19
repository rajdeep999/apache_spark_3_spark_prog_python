import findspark
findspark.init("/home/rajdeep/spark-3.5.0-bin-hadoop3")

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib.utils import *
from lib.log4j import Log4j



def toDate(df,colName, fmt):
    return df.withColumn(colName ,to_date(colName,fmt))


if __name__ == '__main__':
    #get spark config
    conf = get_spark_app_config()

    #initalize and configure spark and logger
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4j(spark)

    #creating data list
    data = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]

    #intializing the schema of df
    schema = StructType(
        [
            StructField('id',StringType()),
            StructField('date',StringType())
        ]
    )

    #creating the df from data with specified schema
    df = spark.createDataFrame(data, schema = schema)

    #verifying the schema and data
    df.printSchema()
    df.show()

    #casting the string date column to date type column
    df_date_fmt = toDate(df,'date','M/d/y')
    
    #verifying the schema and data
    df_date_fmt.printSchema()
    df_date_fmt.show()

    #stopping the spark
    spark.stop()

