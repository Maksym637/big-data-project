"""Module with jobs for the `title.episode` data"""

from pyspark.sql.dataframe import DataFrame

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

    def get_first_latest_title_episodes(self, limit_num=5) -> DataFrame:
        """
        Get first 5 title episodes with the latest season number
        """
        return (
            self.tsv_df
                .groupBy(self.tsv_df[TitleEpisodeModel.parent_tconst])
                .max(TitleEpisodeModel.season_number)
                .limit(num=limit_num)
        )
