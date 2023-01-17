import pyspark.sql
from pyspark.sql.functions import col


def rename_data(input_df: pyspark.sql.DataFrame, column_to_rename_dict: dict) -> pyspark.sql.DataFrame:
    """ Functions renames specific columns in Dataframe. Renames key to value in provided dictionary

    Parameters
    ----------
    input_df
        Dataframe in which to rename the column names
    column_to_rename_dict
        Dictionary with key as old column name and value as a new column name

    Returns
    -------
    result
        Returns renamed Dataframe

    """
    for key, value in column_to_rename_dict.items():
        input_df = input_df.withColumnRenamed(key, value)
    return input_df


def filter_data(input_df, columns_to_filter: dict) -> pyspark.sql.DataFrame:
    """ Function filters specific columns in Dataframe. Filters data from the given dictionary. It uses key as a column
     name, and value as a list that column value should be equal to

    Parameters
    ----------
    input_df
        Dataframe in which to filter data
    columns_to_filter
        Dictionary with key as column name and value is a list that column value should be equal to

    Returns
    -------
    result
        Returns filtered DataFrame

    Examples
    --------
    >>> filtered_df = filter_data(input_df=input_df,columns_to_filter={'country': ['Poland','France'],'active': [True]})

    """
    for key, value in columns_to_filter.items():
        input_df = input_df.filter(col(key).isin(value))
    return input_df


def select_data(input_df: pyspark.sql.DataFrame, columns_to_select: list):
    """ Function return dataframe with only selected columns

    Parameters
    ----------
    input_df
        Dataframe in which to select data
    columns_to_select: list
        A list of columns to select

    Returns
    -------
    result
        Returns Dataframe with selected columns

    """
    input_df = input_df.select(*columns_to_select)
    return input_df
