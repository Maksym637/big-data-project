"""Module with jobs for the `title.principals` data"""

from pyspark.sql import Window, WindowSpec
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import count, col, row_number

from utils.models import TitlePrincipalsModel, TitleRatingsModel
from jobs.title_ratings_job import TitleRatingsData

from jobs.base_job import TSVData


class TitlePrincipalsData(TSVData):
    """Business questions for the `title.principal` data"""

    def get_people_count_on_each_film(self) -> DataFrame:
        """
        Get information about how many people worked on each film
        """
        return self.tsv_df.groupBy(TitlePrincipalsModel.tconst).count()

    def get_first_person_ids_with_biggest_lead_movies_number(
        self,
        category='director',
        limit_num=5
    ) -> DataFrame:
        """
        Get first 5 person ids with the biggest number of lead movies as an director
        """
        return (
            self.tsv_df
                .where(self.tsv_df[TitlePrincipalsModel.category] == category)
                .groupBy(TitlePrincipalsModel.nconst)
                .agg(count("*").alias('count'))
                .orderBy(col('count'), ascending=[False])
                .select(TitlePrincipalsModel.nconst)
                .limit(num=limit_num)
        )

    def get_first_person_id_with_distinct_jobs(self, limit_num=1) -> DataFrame:
        """
        Get id of a person which has the most distinct jobs
        """
        return (
            self.tsv_df
                .groupBy(TitlePrincipalsModel.nconst, TitlePrincipalsModel.category)
                .agg(count(TitlePrincipalsModel.category).alias('count'))
                .groupBy(TitlePrincipalsModel.nconst)
                .agg(count(TitlePrincipalsModel.category).alias('count'))
                .orderBy(col('count'), ascending=[False])
                .limit(num=limit_num)
                .select(TitlePrincipalsModel.nconst)
        )

    def get_directors_of_the_most_popular_titles(
        self,
        title_ratings_data: TitleRatingsData,
        limit_num=10
    ) -> DataFrame:
        """
        Get director's ids of the most popular titles
        """
        return (
            self.tsv_df
                .where(self.tsv_df[TitlePrincipalsModel.category] == 'director')
                .select(TitlePrincipalsModel.tconst, TitlePrincipalsModel.nconst)
                .join(
                    other=(
                        title_ratings_data
                            .tsv_df
                            .select(
                                col(TitleRatingsModel.tconst),
                                col(TitleRatingsModel.average_rating)
                            )
                    ),
                    on=self.tsv_df[TitlePrincipalsModel.tconst] == title_ratings_data.tsv_df[TitleRatingsModel.tconst],
                    how="inner"
                )
                .drop(title_ratings_data.tsv_df[TitleRatingsModel.tconst])
                .orderBy(col(TitleRatingsModel.average_rating), ascending=[False])
                .limit(num=limit_num)
                .select(TitlePrincipalsModel.nconst)
        )

    def get_people_which_had_two_or_more_distinct_job_categories(self, threshold=2):
        """
        Get people which had two or more distinct job categories
        """
        idx_col: str = "idx"
        count_col: str = "count_col"
        window_spec: WindowSpec = (
            Window
                .partitionBy(TitlePrincipalsModel.nconst)
                .orderBy(TitlePrincipalsModel.category)
        )

        return (
            self.tsv_df
                .groupBy(TitlePrincipalsModel.nconst, TitlePrincipalsModel.category)
                .agg(count(TitlePrincipalsModel.nconst).alias(count_col))
                .withColumn(idx_col, row_number().over(window_spec))
                .where(col(idx_col) >= threshold)
                .dropDuplicates([TitlePrincipalsModel.nconst])
                .select(TitlePrincipalsModel.nconst)
        )

    def get_three_titles_per_person(self, threshold=3):
        """
        For each person get no more than three titles in which they worked
        """
        idx_col: str = "idx"
        count_col: str = "count_col"
        window_spec: WindowSpec = (
            Window
                .partitionBy(TitlePrincipalsModel.nconst)
                .orderBy(TitlePrincipalsModel.tconst)
        )

        return (
            self.tsv_df
                .groupBy(TitlePrincipalsModel.nconst, TitlePrincipalsModel.tconst)
                .agg(count(TitlePrincipalsModel.nconst).alias(count_col))
                .withColumn(idx_col, row_number().over(window_spec))
                .where(col(idx_col) <= threshold)
                .select([TitlePrincipalsModel.nconst, TitlePrincipalsModel.tconst])
        )
