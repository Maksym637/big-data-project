"""Module with jobs for the `title.episode` data"""

from pyspark.sql import Window, WindowSpec
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, max, rank

from utils.models import TitleEpisodeModel

from jobs.base_job import TSVData


class TitleEpisodeData(TSVData):
    """Business questions for the `title.episode` data"""

    def get_title_episodes_with_provided_season_number(self, threshold=6) -> DataFrame:
        """
        Get all title episodes where season number is equal 6
        """
        return self.tsv_df.filter(
            condition=(self.tsv_df[TitleEpisodeModel.season_number] == threshold)
        )

    def get_first_title_episodes_in_interval(
        self,
        season_number_threshold=1,
        episode_number_threshold=10,
        limit_num=5
    ) -> DataFrame:
        """
        Get first 5 title episodes where season number is equal 1
        and episode number is less than 10
        """
        return self.tsv_df.filter(
            condition=(
                (self.tsv_df[TitleEpisodeModel.season_number] == season_number_threshold) &
                (self.tsv_df[TitleEpisodeModel.episode_number] < episode_number_threshold)
            )
        ).limit(num=limit_num)

    def get_first_latest_title_episodes(self, limit_num=10) -> DataFrame:
        """
        Get first 10 title episodes with the latest season number
        """
        return (
            self.tsv_df
                .filter(condition=(
                    col(TitleEpisodeModel.parent_tconst).isNotNull() &
                    col(TitleEpisodeModel.season_number).isNotNull()
                ))
                .groupBy(TitleEpisodeModel.parent_tconst)
                .agg(max(TitleEpisodeModel.season_number).alias('max_season_number'))
                .limit(num=limit_num)
        )

    def rank_episodes_within_each_season(self, limit_num=50) -> DataFrame:
        """
        Rank episodes within each season
        """
        window_spec: WindowSpec = (
            Window
                .partitionBy(TitleEpisodeModel.parent_tconst, TitleEpisodeModel.season_number)
                .orderBy(TitleEpisodeModel.episode_number)
        )

        return (
            self.tsv_df
                .filter(condition=(
                    col(TitleEpisodeModel.season_number).isNotNull() &
                    col(TitleEpisodeModel.episode_number).isNotNull()
                ))
                .withColumn(
                    colName='episode_rank',
                    col=rank().over(window_spec)
                )
                .limit(num=limit_num)
        )
