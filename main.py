"""Module in which the entry point to the program is located"""

from pyspark import SparkConf
from pyspark.sql import SparkSession

from utils.constants import INPUT_DATA_PATH
from utils.helpers import write_results_to_file
from utils.schemas import (
    title_episode_schema,
    title_ratings_schema,
    title_principals_schema
)

from jobs.title_episode_job import TitleEpisodeData
from jobs.title_ratings_job import TitleRatingsData
from jobs.title_principals_job import TitlePrincipalsData

spark_session = (
    SparkSession.builder
        .master('local')
        .appName('project_app')
        .config(conf=SparkConf())
        .getOrCreate()
)

if __name__ == '__main__':
    # Import the following data frames
    title_episode_df = TitleEpisodeData(
        spark_session=spark_session,
        path=f'{INPUT_DATA_PATH}/title.episode.tsv',
        schema=title_episode_schema
    )

    title_ratings_df = TitleRatingsData(
        spark_session=spark_session,
        path=f'{INPUT_DATA_PATH}/title.ratings.tsv',
        schema=title_ratings_schema
    )

    title_principals_df = TitlePrincipalsData(
        spark_session=spark_session,
        path=f'{INPUT_DATA_PATH}/title.principals.tsv',
        schema=title_principals_schema
    )

    # Definition of business questions
    title_episode_df.get_first_rows()
    title_ratings_df.get_first_rows()
    title_principals_df.get_first_rows()

    processed_df_list = [
        # BQ for the `title.episode` data
        (title_episode_df.get_title_episodes_with_provided_season_number(), 'title_episode_bq_1'),
        (title_episode_df.get_first_title_episodes_in_interval(), 'title_episode_bq_2'),
        (title_episode_df.get_first_latest_title_episodes(), 'title_episode_bq_3'),
        # BQ for the `title.ratings` data
        (title_ratings_df.count_highly_rated_titles(), 'title_ratings_bq_1'),
        (title_ratings_df.count_titles_with_few_votes(), 'title_ratings_bq_2'),
        (title_ratings_df.get_average_votes_per_rating_interval(), 'title_ratings_bq_3'),
        # BQ for the `title.principals` data
        (title_principals_df.get_people_count_on_each_film(), 'title_principals_bq_1'),
        (title_principals_df.get_first_person_ids_with_biggest_lead_movies_number(), 'title_principals_bq_2'),
        (title_principals_df.get_first_person_id_with_distinct_jobs(), 'title_principals_bq_3')
    ]

    write_results_to_file(processed_df_list)
