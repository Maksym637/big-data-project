"""Module in which the entry point to the program is located"""

from pyspark import SparkConf
from pyspark.sql import SparkSession

from utils.constants import INPUT_DATA_PATH
from utils.helpers import write_results_to_file
from utils.schemas import (
    title_episode_schema,
    title_ratings_schema,
    title_principals_schema,
    title_crew_schema,
    title_basics_schema,
    name_basics_schema,
    title_akas_schema
)

from jobs.title_episode_job import TitleEpisodeData
from jobs.title_ratings_job import TitleRatingsData
from jobs.title_principals_job import TitlePrincipalsData
from jobs.title_crew_job import TitleCrewData
from jobs.title_basics_job import TitleBasicsData
from jobs.name_basics_job import NameBasicsData
from jobs.title_akas_job import TitleAkasData

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

    title_crew_df = TitleCrewData(
        spark_session=spark_session,
        path=f'{INPUT_DATA_PATH}/title.crew.tsv',
        schema=title_crew_schema
    )

    title_basics_df = TitleBasicsData(
        spark_session=spark_session,
        path=f'{INPUT_DATA_PATH}/title.basics.tsv',
        schema=title_basics_schema
    )

    name_basics_df = NameBasicsData(
        spark_session=spark_session,
        path=f'{INPUT_DATA_PATH}/name.basics.tsv',
        schema=name_basics_schema
    )

    title_akas_df = TitleAkasData(
        spark_session=spark_session,
        path=f'{INPUT_DATA_PATH}/title.akas.tsv',
        schema=title_akas_schema
    )

    # Definition of business questions
    title_episode_df.get_first_rows()
    title_ratings_df.get_first_rows()
    title_principals_df.get_first_rows()
    title_crew_df.get_first_rows()
    title_basics_df.get_first_rows()
    name_basics_df.get_first_rows()
    title_akas_df.get_first_rows()

    processed_df_list = [
        # BQ for the `title.episode` data
        (title_episode_df.get_title_episodes_with_provided_season_number(), 'title_episode_bq_1'),
        (title_episode_df.get_first_title_episodes_in_interval(), 'title_episode_bq_2'),
        (title_episode_df.get_first_latest_title_episodes(), 'title_episode_bq_3'),
        (title_episode_df.rank_episodes_within_each_season(), 'title_episode_bq_4'),

        # BQ for the `title.ratings` data
        (title_ratings_df.count_highly_rated_titles(), 'title_ratings_bq_1'),
        (title_ratings_df.count_titles_with_few_votes(), 'title_ratings_bq_2'),
        (title_ratings_df.get_average_votes_per_rating_interval(), 'title_ratings_bq_3'),

        # BQ for the `title.principals` data
        (title_principals_df.get_people_count_on_each_film(), 'title_principals_bq_1'),
        (title_principals_df.get_first_person_ids_with_biggest_lead_movies_number(), 'title_principals_bq_2'),
        (title_principals_df.get_first_person_id_with_distinct_jobs(), 'title_principals_bq_3'),
        (title_principals_df.get_directors_of_the_most_popular_titles(title_ratings_df), 'title_principals_bq_4'),
        (title_principals_df.get_people_which_had_two_or_more_distinct_job_categories(), 'title_principals_bq_5'),
        (title_principals_df.get_three_titles_per_person(), 'title_principals_bq_6'),

        # BQ for the `title.crew` data
        (title_crew_df.get_titles_without_writers(), 'title_crew_bq_1'),
        (title_crew_df.get_top_10_titles_by_crew_count(), 'title_crew_bq_2'),
        (title_crew_df.get_one_man_titles(), 'title_crew_bq_3'),
        (title_crew_df.get_titles_where_director_also_played_character(title_principals_df), 'title_crew_bq_4'),
        (title_crew_df.get_writers_title_rating_rank(title_ratings_df), 'title_crew_bq_5'),
        (title_crew_df.get_directors_total_title_count(), 'title_crew_bq_6'),

        # BQ for the `title.basics` data
        (title_basics_df.count_non_adult_basics_titles(), 'title_basics_bq_1'),
        (title_basics_df.get_count_each_title_type(), 'title_basics_bq_2'),
        (title_basics_df.get_primary_title_where_worked_most_writers(title_crew_df), 'title_basics_bq_3'),
        (title_basics_df.get_title_basics_with_max_runtime_minutes(), 'title_basics_bq_4'),

        # BQ for the `name.basics` data
        (name_basics_df.get_first_alive_specific_persons(), 'name_basics_bq_1'),
        (name_basics_df.count_persons_by_profession(), 'name_basics_bq_2'),
        (name_basics_df.count_persons_by_birth_year_with_number_of_titles(), 'name_basics_bq_3'),
        (name_basics_df.top_actors_average_rating(title_ratings_df), 'name_basics_bq_4'),
        (name_basics_df.rank_youngest_person_for_profession(), 'name_basics_bq_5'),
        (name_basics_df.rank_oldest_person_in_title(), 'name_basics_bq_6')

        # BQ for the `title.akas` data
        (title_akas_df.count_ua_titles(), 'title_akas_bq_1'),
        (title_akas_df.unique_languages(), 'title_akas_bq_2'),
        (title_akas_df.count_titles_by_type(), 'title_akas_bq_3'),
        (title_akas_df.get_original_bowdlerized(), 'title_akas_bq_4'),
        (title_akas_df.rank_by_localizations(), 'title_akas_bq_5'),
        (title_akas_df.rank_by_dvd_count(), 'title_akas_bq_6')
    ]

    write_results_to_file(processed_df_list)
