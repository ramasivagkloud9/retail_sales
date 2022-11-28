import env


class SnowflakeHelper:
    def save_df_to_snowflake(self, df, table):
        """
        The save_df_to_snowflake function saves a dataframe to a table in Snowflake.
        The save_df_to_snowflake function takes two arguments:
            df - the dataframe you want to save
            table - the name of the table you want to save your dataframe into

        :param df: Pass in the dataframe that is to be saved
        :param table: Specify the table name to write the dataframe to
        :return: The number of rows inserted into the table
        """
        sfOptions = {
            "sfURL": env.sfURL,
            "sfAccount": env.sfAccount,
            "sfUser": env.sfUser,
            "sfPassword": env.sfPassword,
            "sfDatabase": env.sfDatabase,
            "sfSchema": env.sfSchema,
            "sfWarehouse": env.sfWarehouse,
            "sfRole": env.sfRole
        }

        df.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(table)).mode(
            "overwrite").options(header=True).save()