"""Module with jobs for the `title.principals` data"""

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import count, col

from utils.models import TitlePrincipalsModel

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
