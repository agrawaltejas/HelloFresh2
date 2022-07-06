from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit

from utilities.spark import start_spark_session
from src.main.extractor.extract import extract_data
import os
from config.definitions import ROOT_DIR
print(ROOT_DIR)

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
    input_df.show(10)
    input_df.printSchema()


    df2 = input_df.filter(input_df.lower(col('ingredients')).contains("beef"))
    df2.select("ingredients").show(10,truncate=False)



    print(f"count : {input_df.count()}")


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
