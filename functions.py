import pyspark.sql
from pyspark.sql.functions import col


def rename_data(input_df: pyspark.sql.DataFrame, column_to_rename_dict: dict) -> pyspark.sql.DataFrame:
    for key, value in column_to_rename_dict.items():
        input_df = input_df.withColumnRenamed(key, value)
    return input_df


def join_data(input1_df: pyspark.sql.DataFrame, input2_df: pyspark.sql.DataFrame, column_to_join_on: str) -> pyspark.sql.DataFrame:
    joined_df = input1_df.join(input2_df, [column_to_join_on])
    return joined_df


def filter_data(input_df, columns_to_filter: dict) -> pyspark.sql.DataFrame:
    for key, value in columns_to_filter.items():
        input_df = input_df.filter(col(key).isin(value))
    return input_df


def select_data(input_df: pyspark.sql.DataFrame, columns_to_select: list):
    input_df = input_df.select(*columns_to_select)
    return input_df
