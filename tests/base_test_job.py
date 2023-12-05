"""Module with the base test class"""

import unittest

from pyspark import SparkConf
from pyspark.sql import SparkSession


class BaseJobTest(unittest.TestCase):
    """Parent test class"""

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark_session = (
            SparkSession.builder
            .master('local')
            .appName('test_project_app')
            .config(conf=SparkConf())
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark_session.stop()
