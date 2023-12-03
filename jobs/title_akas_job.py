"""Module with jobs for the `title.akas` data"""

from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame
from jobs.base_job import TSVData
from utils.models import TitleAkasModel
from pyspark.sql.functions import count, col, dense_rank, percent_rank
from typing import List


class TitleAkasData(TSVData):
    """Business questions for the `title.akas` data"""

    def count_ua_titles(self) -> int:
        """
        Count titles with ukrainian localization
        """

        region = 'UA'

        return(
            self.tsv_df
            .filter(
                col(TitleAkasModel.region) == region
            )
            .count()
        )

    def unique_languages(self) -> List[str]:
        """
        Get unique languages
        """
        
        return(
            [
                i.asDict(True)['language'] for i in
                self.tsv_df
                .select(TitleAkasModel.language)
                .distinct()
                .collect()
            ]
        )

    def count_titles_by_type(self) -> DataFrame:
        """
        Get amount of titles for each title type
        """

        return(
            self.tsv_df
            .groupBy(TitleAkasModel.types)
            .agg(count("*")
            .alias("type_count"))
        )

    def get_original_bowdlerized(self) -> DataFrame:
        """
        Get pairs [original title - bowdlerized title] for all bowdlerized titles
        """

        bowdlerized = 'bowdlerized title'

        return(
            self.tsv_df
            .select(TitleAkasModel.title_id, col(TitleAkasModel.title).alias(bowdlerized))
            .filter(
                col(TitleAkasModel.attributes) == bowdlerized
            )
            .join(
                self.tsv_df
                .select(TitleAkasModel.title_id, col(TitleAkasModel.title).alias("original title"))
                .filter(
                    col(TitleAkasModel.is_original_title) == 1
                ), 
                TitleAkasModel.title_id
            )
        )

    def rank_by_localizations(self) -> DataFrame:
        """
        Rank titles by localization count
        """

        localization_count = 'count'
        windowSpec = Window.orderBy(col(localization_count).desc())

        return(
            self.tsv_df
            .groupBy(TitleAkasModel.title_id)
            .agg(count(TitleAkasModel.ordering)
            .alias(localization_count))
            .withColumn("rank", dense_rank().over(windowSpec))
        )

    def rank_by_dvd_count(self) -> DataFrame:
        """
        Rank regions by count of dvd titles
        """

        dvd_count = "dvd_count"
        windowSpec = Window.orderBy(col(dvd_count).desc())

        return(
            self.tsv_df
            .filter(
                col(TitleAkasModel.types).like('%dvd%')
            )
            .groupBy(TitleAkasModel.region)
            .agg(count(TitleAkasModel.types)
            .alias(dvd_count))
            .withColumn("dvd_rank", percent_rank().over(windowSpec))
        )
