"""Module with jobs for the `title.crew` data"""

from pyspark.sql import Window, WindowSpec
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    concat_ws, collect_list, dense_rank, row_number, explode, count, split, size, col
)

from utils.models import TitleCrewModel, TitlePrincipalsModel, TitleRatingsModel

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

        return (
            self.tsv_df
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
        person_col: str = "person"

        return (
            self.tsv_df
            .filter(col(TitleCrewModel.directors).isNotNull())
            .filter(col(TitleCrewModel.directors) == col(TitleCrewModel.writers))
            .select(TitleCrewModel.tconst, col(TitleCrewModel.directors).alias(person_col))
            .filter(~col(person_col).like("%,%"))
        )

    def get_titles_where_director_also_played_character(self, title_principals_data: TSVData) -> DataFrame:
        """
        Get titles where director also played one of the characters.
        Creates a separate numbered row for each title and director pair.
        """
        director_col: str = "director"
        idx_col: str = "idx"
        window_spec: WindowSpec = Window.partitionBy(TitleCrewModel.tconst).orderBy(director_col)

        return (
            self.tsv_df
            .select(TitleCrewModel.tconst, TitleCrewModel.directors)
            .filter(col(TitleCrewModel.directors).isNotNull())
            .join(
                other=title_principals_data.tsv_df
                .select(
                    col(TitlePrincipalsModel.tconst),
                    col(TitlePrincipalsModel.nconst),
                    col(TitlePrincipalsModel.characters)
                )
                .filter(col(TitlePrincipalsModel.characters).isNotNull()),
                on=self.tsv_df[TitleCrewModel.tconst] == title_principals_data.tsv_df[TitlePrincipalsModel.tconst],
                how="inner"
            )
            .drop(title_principals_data.tsv_df[TitlePrincipalsModel.tconst])
            .filter(col(TitleCrewModel.directors).contains(col(TitlePrincipalsModel.nconst)))
            .select(
                TitleCrewModel.tconst,
                col(TitlePrincipalsModel.nconst).alias(director_col),
                TitlePrincipalsModel.characters
            )
            .withColumn(idx_col, row_number().over(window_spec))
        )

    def get_writers_title_rating_rank(self, title_ratings_data: TSVData) -> DataFrame:
        """
        Get a rank of title each writer worked on based on average title ranking
        """
        writer_col: str = "writer"
        rank_col: str = "rank"
        window_spec: WindowSpec = (
            Window
                .partitionBy(writer_col)
                .orderBy(col(TitleRatingsModel.average_rating).desc())
        )

        return (
            self.tsv_df
            .filter(col(TitleCrewModel.writers).isNotNull())
            .withColumn(writer_col, explode(split(col(TitleCrewModel.writers), ",")))
            .select(TitleCrewModel.tconst, writer_col)
            .join(
                other=title_ratings_data.tsv_df
                .select(
                    col(TitleRatingsModel.tconst),
                    col(TitleRatingsModel.average_rating),
                ),
                on=self.tsv_df[TitleCrewModel.tconst] == title_ratings_data.tsv_df[TitleRatingsModel.tconst],
                how="inner"
            )
            .drop(title_ratings_data.tsv_df[TitleRatingsModel.tconst])
            .withColumn(rank_col, dense_rank().over(window_spec))
        )

    def get_directors_total_title_count(self) -> DataFrame:
        """
        Get the number of titles each director has worked on
        """
        directors_col: str = 'director'
        title_count_col: str = 'title_count'

        return (
            self.tsv_df
            .filter(col(TitleCrewModel.directors).isNotNull())
            .withColumn(directors_col, explode(split(col(TitleCrewModel.directors), ",")))
            .groupBy(directors_col)
            .agg(count(TitleCrewModel.tconst).alias(title_count_col))
            .orderBy(col(title_count_col).desc())
        )
