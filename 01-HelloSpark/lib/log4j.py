class Log4j:
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        root_class = 'learning.spark.example'
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(root_class + '.' + app_name)


    def warn(self, msg):
        self.logger.warn(msg)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)
    
    def debug(self, msg):
        self.logger.debug(msg)