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
    
    def get_top_rated_titles_by_genre(self, genre_table, top_n=10) -> DataFrame:
        """
        Find the top N rated titles in each genre.
        """
        # Joining with genre table
        joined_df = self.tsv_df.join(genre_table, self.tsv_df.titleId == genre_table.titleId, 'inner')

        # Window function to rank titles within each genre based on average rating
        window_spec = Window.partitionBy(genre_table['genre']).orderBy(self.tsv_df[TitleRatingsModel.average_rating].desc())

        # Applying the window function and filtering the top N for each genre
        return (
            joined_df
                .withColumn('rank', F.rank().over(window_spec))
                .filter(F.col('rank') <= top_n)
                .select(genre_table['genre'], self.tsv_df['titleId'], self.tsv_df[TitleRatingsModel.average_rating])
        )
    
    def get_rating_change_over_years(self, titles_table, pivot_year) -> DataFrame:
        """
        Calculate the difference in average rating for titles before and after a specific year.
        """
        # Joining with titles table to get release years
        joined_df = self.tsv_df.join(titles_table, self.tsv_df.titleId == titles_table.titleId, 'inner')

        # Window functions for average ratings before and after the pivot year
        window_spec_before = Window.partitionBy().orderBy().rowsBetween(Window.unboundedPreceding, pivot_year)
        window_spec_after = Window.partitionBy().orderBy().rowsBetween(pivot_year, Window.unboundedFollowing)

        # Applying window functions to calculate average ratings
        return (
            joined_df
                .withColumn('avg_rating_before', F.avg(self.tsv_df[TitleRatingsModel.average_rating]).over(window_spec_before))
                .withColumn('avg_rating_after', F.avg(self.tsv_df[TitleRatingsModel.average_rating]).over(window_spec_after))
                .select('titleId', 'avg_rating_before', 'avg_rating_after')
        )
    
    def get_most_consistent_genres_in_ratings(self, genre_table, min_titles=50) -> DataFrame:
        """
        Identify genres with the least variance in ratings.
        """
        # Joining with genre table
        joined_df = self.tsv_df.join(genre_table, self.tsv_df.titleId == genre_table.titleId, 'inner')

        # Grouping by genre and calculating variance in ratings
        genre_variance_df = (
            joined_df
                .groupBy(genre_table['genre'])
                .agg(F.variance(self.tsv_df[TitleRatingsModel.average_rating]).alias('rating_variance'))
        )

        # Filtering genres with a minimum number of titles
        count_df = joined_df.groupBy(genre_table['genre']).count()
        filtered_genres = count_df.filter(F.col('count') >= min_titles).select('genre')

        # Joining to get consistent genres
        return (
            genre_variance_df
                .join(filtered_genres, 'genre', 'inner')
                .orderBy('rating_variance')
        )

