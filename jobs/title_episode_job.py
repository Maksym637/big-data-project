"""Module with jobs for the `title.episode` data"""

from pyspark.sql.dataframe import DataFrame

from utils.models import TitleEpisodeModel

from jobs.base_job import TSVData


class TitleEpisodeData(TSVData):
    """Business questions for the `title.episode` data"""

    def get_business_question_1(self) -> DataFrame:
        """
        Get all title episodes where season number is equal 6
        """
        return self.tsv_df.filter(
            condition=(self.tsv_df[TitleEpisodeModel.season_number] == 6)
        )

    def get_business_question_2(self) -> DataFrame:
        """
        Get first 5 title episodes where season number is equal 1
        and episode number is less than 10
        """
        return self.tsv_df.filter(
            condition=(
                (self.tsv_df[TitleEpisodeModel.season_number] == 1) &
                (self.tsv_df[TitleEpisodeModel.episode_number] < 10)
            )
        ).limit(num=5)

    def get_business_question_3(self) -> DataFrame:
        """
        Get first 5 title episodes with the latest season number
        """
        return (
            self.tsv_df
                .groupBy(self.tsv_df[TitleEpisodeModel.parent_tconst])
                .max(TitleEpisodeModel.season_number)
                .limit(num=5)
        )
