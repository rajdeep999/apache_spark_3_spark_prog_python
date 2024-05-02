import findspark
findspark.init('/home/rajdeep/spark-3.5.0-bin-hadoop3')

from pyspark.sql import SparkSession
from lib.ConfigLoader import get_spark_config


# initializing the spark session
def get_spark_session(env):
    if env == 'LOCAL':
        return SparkSession.builder \
            .config(conf = get_spark_config(env)) \
            .config('spark.driver.extraJavaOptions',
                    '-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=spark_app') \
            .config('spark.sql.autoBroadcastJoinThreshold','-1') \
            .config('spark.sql.adaptive.enabled','false') \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .enableHiveSupport() \
            .getOrCreate()