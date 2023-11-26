"""Module with jobs for the `title.ratings` data"""

from pyspark.sql.dataframe import DataFrame

from utils.models import TitleRatingsModel

from jobs.base_job import TSVData


class TitleRatingsData(TSVData):
    """Business questions for the `title.ratings` data"""

    def count_highly_rated_titles(self, threshold=8.0) -> int:
        """
        Count how many titles have a rating above a certain threshold
        """
        return self.tsv_df.filter(
            condition=(self.tsv_df[TitleRatingsModel.average_rating] > threshold)
        ).count()

    def count_titles_with_few_votes(self, threshold=100) -> int:
        """
        Count titles with less than a certain number of votes
        """
        return self.tsv_df.filter(
            condition=(self.tsv_df[TitleRatingsModel.number_votes] < threshold)
        ).count()

    def get_average_votes_per_rating_interval(self) -> DataFrame:
        """
        Calculate the average number of votes per rating interval
        """
        return (
            self.tsv_df
                .groupBy(
                    (self.tsv_df[TitleRatingsModel.average_rating] - self.tsv_df[TitleRatingsModel.average_rating] % 1)
                    .alias('rating_interval')
                )
                .agg({'numVotes': 'avg'})
                .orderBy('rating_interval')
        )
