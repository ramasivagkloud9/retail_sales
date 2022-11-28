class LocalHelper:

    def save_df_internal(df, path):
        """
        The save_df_internal function saves a dataframe to the specified path.
        The save_df_internal function takes two arguments: self and df.
        self is the instance of the class that calls this method, which allows for access to attributes and methods within
        the class. The second argument is df, which represents a dataframe object that will be saved in its entirety at
        the specified location.
        :param self: Represent the instance of the class
        :param df: Specify the dataframe to be saved
        :param path: Specify the path to save the dataframe
        :return: A dataframe
        """
        df.coalesce(1).write.option("header",True).mode("overwrite").csv(r'{}'.format(path))