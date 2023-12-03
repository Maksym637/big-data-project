"""Module with jobs for the `name.basics` data"""

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, split, size, count, when, desc, dense_rank, avg, explode
from pyspark.sql.window import Window, WindowSpec

from utils.models import NameBasicsModel, TitleRatingsModel

from jobs.base_job import TSVData
from jobs.title_ratings_job import TitleRatingsData

class NameBasicsData(TSVData):
    """Business questions for the `name.basics` data"""

    def get_first_alive_specific_persons(self, threshold=10) -> DataFrame:
        """
        Get first 10 alive persons whose profession was a producer
        """
        return self.tsv_df.filter(
            condition=(
                    (col(NameBasicsModel.death_year).isNull()) &
                    (col(NameBasicsModel.primary_profession).contains("producer"))
            )
        ).limit(num=threshold)

    def count_persons_by_profession(self, threshold=2) -> DataFrame:
        """
        Get the count of persons for each combination of 2 primary professions
        """
        return self.tsv_df.filter(
            condition=(
                    (col(NameBasicsModel.primary_profession).isNotNull()) &
                    (size(split(col(NameBasicsModel.primary_profession), ',')) == threshold)
            )
        ).groupBy(NameBasicsModel.primary_profession).count().limit(num=10)

    def count_persons_by_birth_year_with_number_of_titles(self, threshold=3) -> DataFrame:
        """
        Get the count of persons for each birth year who have participated in at least 3 titles.
        """
        return (
            self.tsv_df.filter(
                condition=(
                        (col(NameBasicsModel.birth_year).isNotNull()) &
                        (col(NameBasicsModel.known_for_titles).isNotNull()) &
                        (size(split(col(NameBasicsModel.known_for_titles), ',')) >= threshold)
                ))
                .groupBy(NameBasicsModel.birth_year)
                .agg(count(when(col(NameBasicsModel.nconst).isNotNull(), 1))
                .alias("person_count"))
                .orderBy(desc(NameBasicsModel.birth_year))
                .limit(num=10)
        )

    def top_actors_average_rating(self, title_ratings_data: TitleRatingsData, threshold=15) -> DataFrame:
        """
        Find the top 15 actors/actresses with the highest average rating for one title their participate in
        """
        actor_col = "actor"

        return (
            self.tsv_df
            .filter(
                (col(NameBasicsModel.primary_name).isNotNull()) &
                (col(NameBasicsModel.primary_profession).isNotNull()) &
                (col(NameBasicsModel.primary_profession).contains("actor")) | (col(NameBasicsModel.primary_profession).contains("actress"))
            )
            .withColumn(actor_col, explode(split(col(NameBasicsModel.primary_name), ",")))
            .select(NameBasicsModel.known_for_titles, actor_col)
            .join(
                other=title_ratings_data.tsv_df
                .select(
                    col(TitleRatingsModel.tconst),
                    col(TitleRatingsModel.average_rating),
                ),
                on=self.tsv_df[NameBasicsModel.known_for_titles] == title_ratings_data.tsv_df[TitleRatingsModel.tconst],
                how="inner"
            )
            .drop(title_ratings_data.tsv_df[TitleRatingsModel.tconst])
            .groupBy(actor_col, NameBasicsModel.known_for_titles).agg(avg(TitleRatingsModel.average_rating).alias("avg_rating"))
            .orderBy(desc("avg_rating"))
            .limit(num=threshold)
        )

    def rank_youngest_person_for_profession(self, threshold=10) -> DataFrame:
        """
        Rank the youngest person for each profession
        """
        profession_col = "profession"
        rank_col = "rank"
        window_spec: WindowSpec = Window.partitionBy(profession_col).orderBy(desc(NameBasicsModel.birth_year))

        return (
            self.tsv_df
            .filter(
                (col(NameBasicsModel.birth_year).isNotNull()) &
                (col(NameBasicsModel.primary_name).isNotNull()) &
                (col(NameBasicsModel.primary_profession).isNotNull())
            )
            .withColumn(profession_col, explode(split(col(NameBasicsModel.primary_profession), ",")))
            .select(profession_col, NameBasicsModel.primary_name, NameBasicsModel.birth_year)
            .withColumn(rank_col, dense_rank().over(window_spec))
            .limit(num=threshold)
        )

    def rank_oldest_person_in_title(self, threshold=10) -> DataFrame:
        """
        Rank the oldest person for each title
        """
        title_col = "title"
        rank_col = "rank"
        window_spec: WindowSpec = Window.partitionBy(title_col).orderBy(NameBasicsModel.birth_year)

        return (
            self.tsv_df
            .filter(
                (col(NameBasicsModel.birth_year).isNotNull()) &
                (col(NameBasicsModel.primary_name).isNotNull()) &
                (col(NameBasicsModel.known_for_titles).isNotNull())
            )
            .withColumn(title_col, explode(split(col(NameBasicsModel.known_for_titles), ",")))
            .select(title_col, NameBasicsModel.primary_name, NameBasicsModel.birth_year)
            .withColumn(rank_col, dense_rank().over(window_spec))
            .limit(num=threshold)
        )