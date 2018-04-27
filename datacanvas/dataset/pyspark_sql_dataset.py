# -*- coding: utf-8 -*-

import pyspark.sql
from .dataset import DataSet


class PysparkSqlDataSet(DataSet):
    def __init__(self, spark, spec):
        self.__spark = spark

    def get_dataframe(self):
        spark = self.__spark
        spark.read.format
        pass