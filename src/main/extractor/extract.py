"""
extract.py
------------
This code piece is to just extract the data from the source and persist it to a data frame.
For big piece of codes, we need separate extractor,transformer,loader classes to define the functionalities clearly.
"""
from functools import reduce

from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql import Row
from pyspark.sql.types import *
import os
from config.definitions import ROOT_DIR


def extract_data(spark):
    """Load data from Json file format.
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """

    filerange=[]
    for file in os.listdir(os.path.join(ROOT_DIR, 'input')) :
        filerange.append(f"{file}")


    dataframes = map(lambda r: spark.read.json(os.path.join(ROOT_DIR,'input',r)),filerange)
    input_df = reduce(lambda df1, df2: df1.unionAll(df2),dataframes)

    return input_df






