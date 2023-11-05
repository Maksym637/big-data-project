"""Module in which the entry point to the program is located"""

from pyspark import SparkConf
from pyspark.sql import SparkSession

from utils.constants import INPUT_DATA_PATH
from utils.schemas import title_episode_schema

from jobs.title_episode_job import TitleEpisodeData

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

    # Get general information about the data
    title_episode_df.get_first_rows()
    title_episode_df.get_schema_info()
    title_episode_df.get_basic_statistics()

    # Get results to 3 business questions
    title_episode_df.get_business_question_1().show()
    title_episode_df.get_business_question_2().show()
    title_episode_df.get_business_question_3().show()

    # TODO
