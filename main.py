"""Module in which the entry point to the program is located"""

from pyspark import SparkConf
from pyspark.sql import SparkSession

from utils.constants import INPUT_DATA_PATH
from utils.schemas import title_episode_schema

from jobs.title_episode_job import TitleEpisodeData
from jobs.title_ratings_job import TitleRatingsJob

spark_session = (
    SparkSession.builder
        .master('local')
        .appName('project_app')
        .config(conf=SparkConf())
        .getOrCreate()
)

if __name__ == '__main__':
    # Import `title.episode` data
    title_episode_df = TitleEpisodeData(
        spark_session=spark_session,
        path=f'{INPUT_DATA_PATH}/title.episode.tsv',
        schema=title_episode_schema
    )

    # Import `title.ratings` data
    title_ratings_df = TitleRatingsJob(
        spark_session=spark_session,
        path=f'{INPUT_DATA_PATH}/title.ratings.tsv',
        schema=title_ratings_schema
    )

    # Get general information about the data
    title_episode_df.get_first_rows()
    title_episode_df.get_schema_info()
    title_episode_df.get_basic_statistics()

    # Get results to 3 business questions
    title_episode_df.get_business_question_1().show()
    title_episode_df.get_business_question_2().show()
    title_episode_df.get_business_question_3().show()

    # Get general information about the data
    title_ratings_df.get_first_rows()
    title_ratings_df.get_schema_info()
    title_ratings_df.get_basic_statistics()

    # Get results to 3 business questions
    title_ratings_df.get_top_rated_titles().show()
    title_ratings_df.count_highly_rated_titles().show() 
    title_ratings_df.get_most_voted_titles().show()
    title_ratings_df.count_titles_with_few_votes().show()
    title_ratings_df.average_votes_per_rating_interval().show()

    # TODO
