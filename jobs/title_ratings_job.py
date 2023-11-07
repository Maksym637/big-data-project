"""Module with jobs for the `title.ratings` data"""

from jobs.base_job import TSVData

"""
What are the top-rated titles?
How many titles have a rating above 8.0?
What is the distribution of ratings across all titles?
What are the most voted titles?
How many titles have less than 100 votes?
What is the average number of votes per rating interval (e.g., 7.0 - 7.9)?
"""
class TitleRatingsData(TSVData):
    def get_top_rated_titles(self, limit=10):
        """Get the top-rated titles"""
        return self.tsv_df.orderBy(TitleRatingsModel.average_rating.desc()).limit(limit)

    def count_highly_rated_titles(self, threshold=8.0):
        """Count how many titles have a rating above a certain threshold"""
        return self.tsv_df.filter(self.tsv_df[TitleRatingsModel.average_rating] > threshold).count()

    def get_rating_distribution(self):
        """Get distribution of ratings across all titles"""
        return self.tsv_df.groupBy(TitleRatingsModel.average_rating).count().orderBy(TitleRatingsModel.average_rating)

    def get_most_voted_titles(self, limit=10):
        """Get the titles with the most votes"""
        return self.tsv_df.orderBy(TitleRatingsModel.num_votes.desc()).limit(limit)

    def count_titles_with_few_votes(self, threshold=100):
        """Count titles with less than a certain number of votes"""
        return self.tsv_df.filter(self.tsv_df[TitleRatingsModel.num_votes] < threshold).count()

    def average_votes_per_rating_interval(self):
        """Calculate the average number of votes per rating interval"""
        return (
            self.tsv_df
                .groupBy((self.tsv_df[TitleRatingsModel.average_rating] - self.tsv_df[TitleRatingsModel.average_rating] % 1).alias('rating_interval'))
                .agg({'numVotes': 'avg'})
                .orderBy('rating_interval')
        )
