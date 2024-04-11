
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType,TimestampType
from pyspark.sql.functions import col, month, year, dayofweek, col, lit, when, cast,to_timestamp
import pandas as pd
import pymongo
import argparse

parser = argparse.ArgumentParser(description="Uber Data Pipeline")
parser.add_argument("data_path", type=str, help="Path to the input data file")

def create_spark_dataframe(data_path):
    """Creates a Spark DataFrame from a CSV file, handling errors gracefully."""
    try:
        # Explicitly create a SparkSession for clarity and consistency:
        spark = SparkSession.builder.appName("DataFrameLoader").getOrCreate()

        # Ensure data_path is a string:
        data_path = "data/uber_data.csv"  # Replace with your data path

        # Read the CSV file, handling potential encoding issues:
        df = spark.read.option("header", True).option("encoding", "UTF-8").csv(data_path)

        return df

    except Exception as e:
        print("Error reading data:", e)
        # Log the error for more detailed analysis (optional):
        import logging
        logging.error("Error reading data from CSV:", exc_info=True)
        return None

#feature calculation example
def add_duration_column(df):
    """Adds a 'duration' column to a PySpark DataFrame, calculating the difference in seconds between tpep_pickup_datetime and tpep_dropoff_datetime."""
    return df.withColumn(
        "duration",
        F.unix_timestamp(F.col("tpep_dropoff_datetime")) - F.unix_timestamp(F.col("tpep_pickup_datetime"))
    )

def add_rate_code_name_column(df):
    """
    Adds a 'rate_code_name' column to a PySpark DataFrame based on the 'RatecodeID' column.
    """
    rate_code_type = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride"
    }
  

    df = df.withColumn("rate_code_name", 
                       F.when(F.col("RatecodeID").isin(list(rate_code_type.keys())), 
                              F.create_map(*[F.lit(k) for k in rate_code_type.keys()])
                                  .getItem(F.col("RatecodeID"))
                             )
                      )
  
        
    return df
  



def add_datetime_columns(df):
    """
    Adds 'month', 'year', and 'weekday' columns to a PySpark DataFrame based on the 'tpep_pickup_datetime' column.
    """
    "Not really necessary in this case,but this is one way of handling a change in schema type"
    
    df = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))
    # df = df.withColumn("tpep_pickup_datetime", cast(col("tpep_pickup_datetime"), TimestampType()))
    # df = df.withColumn("tpep_pickup_datetime", cast(col("tpep_pickup_datetime"), TimestampType()))
# 
    df = df.withColumn("month", month(col("tpep_pickup_datetime"))) \
           .withColumn("year", year(col("tpep_pickup_datetime"))) \
           .withColumn("weekday", dayofweek(col("tpep_pickup_datetime")))

    return df




def store_to_mongodb(df, mongo_uri, mongo_database, mongo_collection):
    """Stores a PySpark DataFrame to MongoDB."""
    try:
        # Convert PySpark DataFrame to Pandas DataFrame
        pandas_df = df.toPandas()

        # Connect to MongoDB
        client = pymongo.MongoClient(mongo_uri)
        db = client[mongo_database]
        collection = db[mongo_collection]

        # Convert Pandas DataFrame to dictionary for insertion
        records = pandas_df.to_dict(orient='records')

        # Insert records into MongoDB collection
        collection.insert_many(records)

        print("Data stored to MongoDB successfully!")

    except Exception as e:
        print("Error storing data to MongoDB:", e)


def run_pipeline(data_path, mongo_uri, mongo_database, mongo_collection):
    """Triggers the data processing pipeline"""
    spark = SparkSession.builder.appName("UberDataPipeline").getOrCreate()
    try:
        args = parser.parse_args()
        raw_df = create_spark_dataframe(data_path)
        if raw_df is None:
            return
        raw_df.createOrReplaceTempView("UBER_DATA")
        transformed_df = add_duration_column(raw_df)
        transformed_df = add_rate_code_name_column(transformed_df)
        transformed_df= add_datetime_columns(transformed_df)


        # Store transformed data to MongoDB
        store_to_mongodb(transformed_df, mongo_uri, mongo_database, mongo_collection)

    except Exception as e:
        print("Error during processing:", e)
    finally:
        spark.stop()


if __name__ == "__main__":
    # data_path = "data/uber_data.csv"
    args = parser.parse_args()
    data_path = args.data_path
    mongo_uri = "mongodb://localhost:27017/"
    mongo_database = "uber_data_db"
    mongo_collection = "uber_data_collection"
    run_pipeline(data_path, mongo_uri, mongo_database, mongo_collection)
