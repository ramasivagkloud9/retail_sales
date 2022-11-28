import sys
from pathlib import Path
sys.path.append(str(Path.cwd().parent))

from helpers.spark_helper import SparkHelper
from helpers.snowflake_helper import SnowflakeHelper
from helpers.local_helper import LocalHelper
from helpers.hive_helper import HiveHelper

import pyspark.sql.functions as F

def load_csv():
    spark = SparkHelper.get_spark_session()
    cleansed_df = spark.read.csv(r"outputs\raw_layer.csv", header=True).coalesce(1)
    cleansed_df = cleansed_df.withColumn("Orderdate", F.to_date("Orderdate", "M/d/yyyy")) \
        .withColumn("Duedate", F.to_date("Duedate", "M/d/yyyy")) \
        .withColumn("Shipdate", F.to_date("Shipdate", "M/d/yyyy"))
    cleansed_df = cleansed_df.select(F.col("OrderNumber"),
                    F.split(F.col("ProductName"), ",").getItem(0).alias("ProductName"),
                    F.col("Color"),
                    F.col("Category"),
                    F.col("Subcategory"),
                    F.col("ListPrice"),
                    F.col("Orderdate"),
                    F.col("Duedate"),
                    F.col("Shipdate"),
                    F.col("PromotionName"),
                    F.col("SalesRegion"),
                    F.col("OrderQuantity"),
                    F.col("UnitPrice"),
                    F.col("SalesAmount"),
                    F.col("DiscountAmount"),
                    F.col("TaxAmount"))
    cleansed_df = cleansed_df\
        .withColumn("Category", F.concat_ws(' - ',"Category","Subcategory")) \
        .drop("Subcategory")\
        .withColumn('OrderQuantity', F.regexp_replace('OrderQuantity', 'Nan', '1')) \
        .withColumn('OrderQuantity', F.col('OrderQuantity').cast('int')) \
        .withColumn("ListPrice", F.round("ListPrice", 2)) \
        .withColumn("UnitPrice", F.round("UnitPrice", 2)) \
        .withColumn("SalesAmount", F.round("SalesAmount", 2)) \
        .withColumn("DiscountAmount", F.round("DiscountAmount", 2)) \
        .withColumn("TaxAmount", F.round("TaxAmount", 2)) \
        .na.fill("NA")
    return cleansed_df

def to_local(df):
    LocalHelper.save_df_internal(df, r"outputs\cleansed_layer.csv")

def to_hive(df):
    HiveHelper().create_hive_database(SparkHelper.get_spark_session(), "retail")
    HiveHelper().save_data_in_hive(df, "retail", "cleansed_layer")

def to_snowflake(df):
    SnowflakeHelper().save_df_to_snowflake(df, "retail_cleansed_layer")

if __name__ == "__main__":
    df = load_csv()
    to_local(df)
    to_hive(df)
    to_snowflake(df)