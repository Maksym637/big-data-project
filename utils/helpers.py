"""Module with helper methods"""

from pyspark.sql.dataframe import DataFrame

from utils.constants import OUTPUT_DATA_PATH


def write_results_to_file(processed_df_list: list) -> None:
    """Write results to file"""
    for processed_df, folder_df in processed_df_list:
        if isinstance(processed_df, DataFrame):
            processed_df.write.csv(
                path=f'{OUTPUT_DATA_PATH}/{folder_df}', header=True, mode='overwrite'
            )
        else:
            with open(
                file=f'{OUTPUT_DATA_PATH}/{folder_df}/{folder_df}.txt', mode='w'
            ) as file:
                file.write(str(processed_df))
