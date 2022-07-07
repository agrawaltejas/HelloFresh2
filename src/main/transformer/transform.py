"""
transform.py
------------
This code piece is to transform the data from the persisted input_df created after extracting the data from source.
For big piece of codes, we need separate extractor,transformer,loader classes to define the functionalities clearly.
"""


from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as sql_fun
import os
from config.definitions import ROOT_DIR


def transform_data(input_df):

    df_with_beef = input_df.filter(sql_fun.lower(input_df.ingredients).contains("beef"))

    # Converting cookTime and prepTime in seconds
    df2 = df_with_beef.withColumn(
        'cookTime',
        sql_fun.coalesce(sql_fun.regexp_extract('cookTime', r'(\d+)H', 1).cast('int'), sql_fun.lit(0)) * 3600 +
        sql_fun.coalesce(sql_fun.regexp_extract('cookTime', r'(\d+)M', 1).cast('int'), sql_fun.lit(0)) * 60 +
        sql_fun.coalesce(sql_fun.regexp_extract('cookTime', r'(\d+)S', 1).cast('int'), sql_fun.lit(0))
    ).withColumn(
        'prepTime',
        sql_fun.coalesce(sql_fun.regexp_extract('prepTime', r'(\d+)H', 1).cast('int'), sql_fun.lit(0)) * 3600 +
        sql_fun.coalesce(sql_fun.regexp_extract('prepTime', r'(\d+)M', 1).cast('int'), sql_fun.lit(0)) * 60 +
        sql_fun.coalesce(sql_fun.regexp_extract('prepTime', r'(\d+)S', 1).cast('int'), sql_fun.lit(0))
    )

    # calculating total_cook_time in minutes
    df3 = df2.withColumn(
        'total_cook_time',
        (expr("cookTime + prepTime")/60).cast(IntegerType())
             )

    # calculating difficulty on the basis of total_cook_time
    df4 = df3.withColumn("difficulty", when(df3.total_cook_time < 30, "easy")
                        .when((df3.total_cook_time >= 30) & (df3.total_cook_time < 60), "medium")
                        .when(df3.total_cook_time >= 60, "hard"))

    return df4
