import env


class HiveHelper:

    def create_hive_database(self, spark, hive_db):
        """
        The create_hive_database function creates a Hive database in the Spark session.
        It takes two arguments: spark and hive_db.
        :param self: Reference the class instance
        :param spark: Execute the sql command
        :param hive_db: Create a hive database
        :return: A string
        """
        spark.sql("create database IF NOT EXISTS {}".format(hive_db))

    def save_data_in_hive(self, df, hive_db, hive_table):
        """
        The save_data_in_hive function saves the data in a DataFrame to a Hive table.
        The function takes three arguments:
            df - The DataFrame that will be saved to Hive.
            hive_db - The name of the database where the table is located in Hive.This is not checked for validity here,
             but rather when calling this function from another Python script or notebook.
             If you are running this as an action on Databricks,
             then you can use one of the pre-existing databases like &quot;default&quot;.
            hive_table - The name of the table within Hive where we want to save our data.
        :param self: Access the attributes and methods of the class in python
        :param df: Specify the dataframe to be saved in hive
        :param hive_db: Specify the database in hive where the data will be stored
        :param hive_table: Specify the name of the table in hive
        :return: The dataframe
        """
        df.coalesce(1).write.mode("append").saveAsTable("{}.{}".format(hive_db, hive_table))