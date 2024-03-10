class Log4J:
    def __init__(self,spark):
        log4j = spark._jvm.org.apache.log4j
        root_name = 'learning.spark.example'
        spark_conf = spark.sparkContext.getConf()
        app_name = spark_conf.get('spark.app.name')
        self.logger = log4j.LogManager.getLogger(root_name+'.'+app_name)

    def warn(self,msg):
        self.logger.warn(msg)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)

    def debug(self, msg):
        self.logger.debug(msg)
