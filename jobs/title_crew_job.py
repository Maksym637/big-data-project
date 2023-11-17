"""Module with jobs for the `title.crew` data"""

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import concat_ws, collect_list, explode, size, split, col

from utils.models import TitleCrewModel

from jobs.base_job import TSVData


class TitleCrewData(TSVData):
    """Business questions for the `title.crew` data"""

    def get_titles_without_writers(self) -> DataFrame:
        """
        Get all titles without writers in their crew
        """
        return self.tsv_df.filter(col(TitleCrewModel.writers).isNull())

    def get_top_10_titles_by_crew_count(self) -> DataFrame:
        """
        Get top 10 titles with the most crew members (directors + writers, count unique only)
        """
        all_crew_col: str = "whole_crew"
        crew_member_col: str = "crew_member"
        total_count_col: str = "total_count"
        return (self.tsv_df
                .withColumn(all_crew_col, concat_ws(",", col(TitleCrewModel.directors), col(TitleCrewModel.writers)))
                .withColumn(crew_member_col, explode(split(col(all_crew_col), ",")))
                .select(TitleCrewModel.tconst, crew_member_col)
                .distinct()
                .groupBy(TitleCrewModel.tconst)
                .agg(concat_ws(",", collect_list(crew_member_col)).alias(all_crew_col))
                .withColumn(total_count_col, size(split(col(all_crew_col), ",")))
                .orderBy(col(total_count_col).desc())
                .limit(10)
                )

    def get_one_man_titles(self) -> DataFrame:
        """
        Get titles where only one person worked both as director and writer
        """
        crew_col: str = "crew"
        return (self.tsv_df
                .filter(col(TitleCrewModel.directors) == col(TitleCrewModel.writers))
                .select(TitleCrewModel.tconst, col(TitleCrewModel.directors).alias(crew_col))
                .filter(~col(crew_col).like("%,%"))
                )
