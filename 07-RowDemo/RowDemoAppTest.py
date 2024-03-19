from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

from lib.utils import *
from unittest import TestCase
from datetime import date


class RowDemoApptest(TestCase):

    @classmethod
    def setUpClass(cls):
        #initalize and configure spark
        cls.spark = SparkSession.builder.master('local[3]').appName("RowDemoTest").getOrCreate()
        
        #creating data list
        data = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]

        #intializing the schema of df
        schema = StructType([StructField('id',StringType()), StructField('date',StringType())])

        #creating the df from data with specified schema
        df = cls.spark.createDataFrame(data,schema=schema)
        
        #casting the string date column to date type column
        cls.df = df.withColumn('date',to_date('date','M/d/y'))

    def test_data_typ(self):
        rows = self.df.collect()
        #verifying the datatype of date column
        for row in rows:
            self.assertIsInstance(row['date'],date)

    
    def test_date_value(self):
        rows = self.df.collect()
        #verifying the data presnt in date column
        for row in rows:
            self.assertEqual(row['date'],date(2020,4,5))

    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

