from unittest import TestCase

import findspark
findspark.init("/home/rajdeep/spark-3.5.0-bin-hadoop3")
from pyspark.sql import *

from lib.utils import *


class utilsTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_spark = SparkSession.builder.appName("testSpark").master("local[3]").getOrCreate()

    def test_row(self):
        load_df = load_survey_df(self.test_spark,"data/sample.csv")
        self.assertEqual(load_df.count(),9,"Record count should be 9")

    def test_country_count(self):
        load_df = load_survey_df(self.test_spark,"data/sample.csv")
        country_df = count_by_country(load_df).collect()
        country_dict = {}

        for row in country_df:
            country_dict[row['country']] = row['count']

        self.assertEqual(country_dict['United States'],4,"Count for United States should be 4")
        self.assertEqual(country_dict['Canada'],2,"Count for United States should be 4")
        self.assertEqual(country_dict['United Kingdom'],1,"Count for United States should be 4")


    @classmethod
    def tearDownClass(cls):
        cls.test_spark.stop()
