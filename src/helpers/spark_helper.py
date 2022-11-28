from pyspark.sql import SparkSession

class SparkHelper:
    def get_spark_session():
        return SparkSession.builder.enableHiveSupport().config('spark.jars.packages',
                                                     'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3').getOrCreate()
