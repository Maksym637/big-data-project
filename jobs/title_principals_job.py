"""Module with jobs for the `title.principals` data"""

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import count,col,row_number
from pyspark.sql.window import Window

from utils.models import TitlePrincipalsModel

from jobs.base_job import TSVData

class TitlePrincipalsData(TSVData):
  """Business questions for the `title.principal` data"""

  def get_business_question_1(self) -> DataFrame:
        """
        Get information about how many people worked on each film
        """
        return self.tsv_df.groupBy(TitlePrincipalsModel.tconst).count()

  def get_business_question_2(self) -> DataFrame:
      """
      Get first 5 person ids with the biggest nubmer of lead movies as an director
      """
      return (self.tsv_df
                .where(self.tsv_df[TitlePrincipalsModel.category] == 'director')
                .groupBy(TitlePrincipalsModel.nconst)
                .agg(count("*").alias('count'))
                .orderBy(col('count'), ascending=[False])
                .select(TitlePrincipalsModel.nconst)
                .limit(num=5))

  def get_business_question_3(self) -> DataFrame:
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
            .limit(num=1)
            .select(TitlePrincipalsModel.nconst)
      )
  
  def get_business_question_4(self) -> DataFrame:
      """
      
      """
      return (self.tsv_df
              
              )
  
  def get_business_question_5(self) -> DataFrame:
      """
      
      """
      return (self.tsv_df
              
              )


  def get_business_question_6(self) -> DataFrame:
      """
      
      """
      return (self.tsv_df
              
              )