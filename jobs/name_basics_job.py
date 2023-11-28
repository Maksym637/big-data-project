"""Module with jobs for the `name.basics` data"""

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, split, size, count, when, desc

from utils.models import NameBasicsModel

from jobs.base_job import TSVData


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
        ).show(n=threshold)

    def count_persons_by_profession(self, threshold=2) -> DataFrame:
        """
        Get the count of persons for each combination of 2 primary professions
        """
        return self.tsv_df.filter(
            condition=(
                    (col(NameBasicsModel.primary_profession).isNotNull()) &
                    (size(split(col(NameBasicsModel.primary_profession), ',')) == threshold)
            )
        ).groupBy(NameBasicsModel.primary_profession).count().show(n=10)

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
                .show(n=10)
        )