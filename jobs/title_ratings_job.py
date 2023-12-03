"""Module with jobs for the `title.ratings` data"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

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
    
    def running_total_and_average(self):
        """
        Calculate the running total of votes and the average rating for each title,
        ordered by average rating in descending order.
        """
        window_spec = Window.orderBy(F.desc("averageRating"))
        return (
            self.tsv_df
                .withColumn("RunningTotalVotes", F.sum("numVotes").over(window_spec))
                .withColumn("RunningAvgRating", F.avg("averageRating").over(window_spec))
                .select("tconst", "averageRating", "numVotes", "RunningTotalVotes", "RunningAvgRating")
        )
    
    def highest_rating_in_groups_of_five(self):
        """
        Identify the highest average rating within each group of 5 titles,
        sorted by number of votes in descending order.
        """
        window_spec = Window.orderBy(F.desc("numVotes")).rowsBetween(0, 4)
        return (
            self.tsv_df
                .withColumn("HighestRatingInGroup", F.max("averageRating").over(window_spec))
                .select("tconst", "averageRating", "numVotes", "HighestRatingInGroup")
        )

