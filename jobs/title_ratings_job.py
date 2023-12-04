"""Module with jobs for the `title.ratings` data"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import sum, avg, max, desc, split, array_contains

from utils.models import TitleCrewModel, TitleRatingsModel

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
        running_total_votes_col: str = 'running_total_votes'
        running_avg_rating_col: str = 'running_avg_rating'
        window_spec = Window.orderBy(desc(TitleRatingsModel.average_rating))

        return (
            self.tsv_df
                .withColumn(running_total_votes_col, sum(TitleRatingsModel.number_votes).over(window_spec))
                .withColumn(running_avg_rating_col, avg(TitleRatingsModel.average_rating).over(window_spec))
                .select(
                    TitleRatingsModel.tconst,
                    TitleRatingsModel.average_rating,
                    TitleRatingsModel.number_votes,
                    running_total_votes_col,
                    running_avg_rating_col
                )
        )
    
    def highest_rating_in_groups_of_five(self):
        """
        Identify the highest average rating within each group of 5 titles,
        sorted by number of votes in descending order.
        """
        highest_rating_group_col: str = 'highest_rating_group'
        window_spec = Window.orderBy(desc(TitleRatingsModel.number_votes)).rowsBetween(0, 4)

        return (
            self.tsv_df
                .withColumn(highest_rating_group_col, max(TitleRatingsModel.average_rating).over(window_spec))
                .select(
                    TitleRatingsModel.tconst,
                    TitleRatingsModel.average_rating,
                    TitleRatingsModel.number_votes,
                    highest_rating_group_col
                )
        )
    
    def average_rating_by_director(self, title_crew_data, director_id) -> float:
        """
        Find the average rating of movies directed by a specific director.
        :param title_crew_data: DataFrame of title crew data.
        :param director_id: The unique identifier of the director.
        :return: The average rating of their movies.
        """
        joined_df = self.tsv_df.join(
            other=title_crew_data.tsv_df,
            on=self.tsv_df[TitleRatingsModel.tconst] == title_crew_data.tsv_df[TitleCrewModel.tconst],
            how='inner'
        )

        director_movies_df = joined_df.filter(
            array_contains(split(title_crew_data.tsv_df[TitleCrewModel.directors], ','), director_id)
        )

        return director_movies_df.agg(avg(TitleRatingsModel.average_rating)).first()[0]

