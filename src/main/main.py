"""
This code piece contains the main entry point of the Application.
It consolidates the data from all modules, executes and saves the final output.
"""

import sys
import os
ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '..\\..'))
sys.path.append(ROOT_DIR)
print(sys.path)

from utilities.spark import start_spark_session
from src.main.extractor.extract import extract_data
from src.main.transformer.transform import transform_data
# from config.definitions import ROOT_DIR


def main():
    """Main ETL script definition.
    :return: None
    """

    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark_session(
        app_name='my_etl_job'
        )

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # start ETL Pipeline
    # Extract data
    input_df = extract_data(spark)
    input_df.persist()
    # input_df.show(10)
    # input_df.printSchema()
    # print(f"count : {input_df.count()}")


    df2 = transform_data(input_df)
    df2.show(20)
    print(f"df2 count : {df2.count()}")


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
