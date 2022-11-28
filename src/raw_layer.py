import sys
from pathlib import Path
sys.path.append(str(Path.cwd().parent))

from helpers.spark_helper import SparkHelper
from helpers.snowflake_helper import SnowflakeHelper
from helpers.local_helper import LocalHelper

def load_csv():
    spark=SparkHelper.get_spark_session()
    raw_df = spark.read.csv(r"D:\retail_sales\src\inputs\Retail.csv", header=True).coalesce(1)
    # raw_df = raw_df.na.drop()
    return raw_df

def to_local(df):
    LocalHelper.save_df_internal(df, r"outputs\raw_layer.csv")

def to_snowflake(df):
    SnowflakeHelper().save_df_to_snowflake(df, "retail_raw_layer")

if __name__=="__main__":
    df=load_csv()
    to_local(df)
    to_snowflake(df)
# raw = spark.read.csv("/content/drive/MyDrive/Retail.csv", header=True)
# raw.show()
#
# raw.printSchema()
#
# """# cleansed"""
#
# from pyspark.sql.functions import *
# from pyspark.sql.functions import to_timestamp
# from pyspark.sql.types import *
# import pyspark.sql as F
#
# df = raw.withColumn("Orderdate", to_date("Orderdate", "M/d/yyyy")) \
#     .withColumn("Duedate", to_date("Duedate", "M/d/yyyy")) \
#     .withColumn("Shipdate", to_date("Shipdate", "M/d/yyyy")) \
#  \
#     df.show()
#
# df.printSchema()
#
# import pyspark.sql.functions as F
#
# slt = df.select(F.col("OrderNumber"),
#                 F.split(F.col("ProductName"), ",").getItem(0).alias("ProductName"),
#                 F.split(F.col("ProductName"), ",").getItem(1).alias("size"),
#                 F.col("Color"),
#                 F.col("Category"),
#                 F.col("ListPrice"),
#                 F.col("Orderdate"),
#                 F.col("Duedate"),
#                 F.col("Shipdate"),
#                 F.col("PromotionName"),
#                 F.col("SalesRegion"),
#                 F.col("OrderQuantity"),
#                 F.col("UnitPrice"),
#                 F.col("SalesAmount"),
#                 F.col("DiscountAmount"),
#                 F.col("TaxAmount"))
#
# slt.show()
#
# b = slt.withColumn('ProductName', F.regexp_replace('ProductName', '- ', '')) \
#     .na.fill("Na") \
#     .withColumn('OrderQuantity', col('OrderQuantity').cast('int')) \
#     .withColumn('size', regexp_replace('size', 'null', 'Na'))
#
# b.show(20, truncate=False)
#
# a = b.write.csv("sunil1.csv", header=True)
#
# """#curated"""
#
# df3 = b.withColumn('Discount_Present', when(col('DiscountAmount') == '0', "Y").otherwise("N")) \
#     .drop("DiscountAmount")
#
# df3.show()
#
# df3.write.csv("sunil2.csv", header=True)
#
# """#Aggregation"""
#
# a = df3.groupBy("Category").agg(sum("OrderQuantity").alias("sold_category"))
#
# a.show()
#
# a.write.csv("sunil3.csv", header=True)
#
# sold = df3.groupBy("SalesRegion").agg(sum("OrderQuantity").alias("sold_region"))
#
# sold.show()
#
# sold.write.csv("sunil4.csv", header=True)
