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
    curated_df = spark.read.csv(r"outputs\cleansed_layer.csv", header=True).coalesce(1)
    curated_df = curated_df.withColumn('DiscountAmount', F.when(F.col('DiscountAmount') == 0, "N").otherwise("Y")) \
                .withColumnRenamed('DiscountAmount', 'Discount_present')
    return curated_df

def load_agg_region_csv():
    spark = SparkHelper.get_spark_session()
    curated_df = spark.read.csv(r"outputs\curated_layer.csv", header=True).coalesce(1)
    curated_df = curated_df.groupBy("SalesRegion").agg(F.sum("OrderQuantity").alias("OrderQuantity"))
    return curated_df

def load_agg_category_csv():
    spark = SparkHelper.get_spark_session()
    curated_df = spark.read.csv(r"outputs\curated_layer.csv", header=True).coalesce(1)
    curated_df = curated_df.groupBy("Category").agg(F.sum("OrderQuantity").alias("OrderQuantity"))
    return curated_df

def to_local(df):
    LocalHelper.save_df_internal(df, r"outputs\curated_layer.csv")

def region_to_local(df):
    LocalHelper.save_df_internal(df, r"outputs\agg_region.csv")

def category_to_local(df):
    LocalHelper.save_df_internal(df, r"outputs\agg_category.csv")

def to_snowflake(df):
    SnowflakeHelper().save_df_to_snowflake(df, "retail_curated_layer")

def region_to_snowflake(df):
    SnowflakeHelper().save_df_to_snowflake(df, "retail_agg_region")

def category_to_snowflake(df):
    SnowflakeHelper().save_df_to_snowflake(df, "retail_agg_category")

def to_hive(df, table):
    HiveHelper().create_hive_database(SparkHelper.get_spark_session(), "retail")
    HiveHelper().save_data_in_hive(df, "retail", table)

if __name__ == "__main__":
    df = load_csv()
    to_local(df)
    to_hive(df, "curated_layer")
    to_snowflake(df)

    df = load_agg_region_csv()
    region_to_local(df)
    to_hive(df, "agg_region")
    region_to_snowflake(df)

    df = load_agg_category_csv()
    category_to_local(df)
    to_hive(df, "agg_category")
    category_to_snowflake(df)
