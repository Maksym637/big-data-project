"""Module with jobs for the `title.basics` data"""

import pyspark.sql.types as t
from pyspark.sql import Window, WindowSpec
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    explode, count, split, col, max, min
)

from utils.models import TitleBasicsModel, TitleCrewModel

from jobs.base_job import TSVData
from jobs.title_crew_job import TitleCrewData


class TitleBasicsData(TSVData):
    """Business questions for the `title.basics` data"""

    def count_non_adult_basics_titles(self) -> int:
        """
        Get count of all non-adult basics titles
        """
        return (
            self.tsv_df
                .withColumn(
                    colName=TitleBasicsModel.is_adult,
                    col=col(TitleBasicsModel.is_adult).cast(dataType=t.BooleanType())
                )
                .filter(condition=(col(TitleBasicsModel.is_adult) == False))
                .count()
        )

    def get_count_each_title_type(self) -> DataFrame:
        """
        Group all title types by count in descending order
        """
        count_title_types_col: str = 'count_title_types'

        return (
            self.tsv_df
                .groupBy(TitleBasicsModel.title_type)
                .agg(count('*').alias(count_title_types_col))
                .orderBy(col(count_title_types_col).desc())
        )

    def get_primary_title_where_worked_most_writers(
        self,
        title_crew_data: TitleCrewData
    ) -> DataFrame:
        """
        Get the primary title of the title on which the most number of writers worked
        """
        tconst_basics: str = TitleBasicsModel.tconst
        tconst_crew: str = TitleCrewModel.tconst
        writers_number_col: str = 'writers_number'

        return (
            self.tsv_df
                .join(
                    other=title_crew_data.tsv_df,
                    on=(self.tsv_df[tconst_basics] == title_crew_data.tsv_df[tconst_crew]),
                    how='inner'
                )
                .withColumn(
                    colName=TitleCrewModel.writers,
                    col=explode(split(col(TitleCrewModel.writers), ','))
                )
                .groupBy(TitleBasicsModel.primary_title)
                .agg(count(TitleCrewModel.writers).alias(writers_number_col))
                .orderBy(col(writers_number_col).desc())
                .first()
        )

    def get_title_basics_with_max_runtime_minutes(self, limit_num=50) -> DataFrame:
        """
        Get first 50 title basics rows with the column that will contain
        maximum of the runtimeMinutes
        """
        runtime_minutes_max_col: str = 'runtime_minutes_max'
        window_spec: WindowSpec = Window.partitionBy()

        return (
            self.tsv_df
                .filter(condition=col(TitleBasicsModel.runtime_minutes).isNotNull())
                .withColumn(
                    colName=runtime_minutes_max_col,
                    col=max(col(TitleBasicsModel.runtime_minutes)).over(window_spec)
                )
                .select(
                    TitleBasicsModel.tconst,
                    TitleBasicsModel.title_type,
                    TitleBasicsModel.primary_title,
                    TitleBasicsModel.original_title,
                    TitleBasicsModel.runtime_minutes,
                    runtime_minutes_max_col
                )
                .limit(num=limit_num)
        )

    def get_min_max_start_end_years_for_each_title_type(self) -> DataFrame:
        """
        Get the minimum and maximum starting and ending year for each content title type
        """
        min_start_year_col: str = 'min_start_year'
        max_start_year_col: str = 'max_start_year'
        min_end_year_col: str = 'min_end_year'
        max_end_year_col: str = 'max_end_year'
        window_spec: WindowSpec = Window.partitionBy(TitleBasicsModel.title_type)

        return (
            self.tsv_df
                .withColumn(
                    colName=min_start_year_col,
                    col=min(col(TitleBasicsModel.start_year)).over(window_spec)
                )
                .withColumn(
                    colName=max_start_year_col,
                    col=max(col(TitleBasicsModel.start_year)).over(window_spec)
                )
                .withColumn(
                    colName=min_end_year_col,
                    col=min(col(TitleBasicsModel.end_year)).over(window_spec)
                )
                .withColumn(
                    colName=max_end_year_col,
                    col=max(col(TitleBasicsModel.end_year)).over(window_spec)
                )
                .select(
                    TitleBasicsModel.tconst,
                    TitleBasicsModel.title_type,
                    TitleBasicsModel.primary_title,
                    TitleBasicsModel.original_title,
                    TitleBasicsModel.start_year,
                    TitleBasicsModel.end_year,
                    min_start_year_col,
                    max_start_year_col,
                    min_end_year_col,
                    max_end_year_col
                )
        )
