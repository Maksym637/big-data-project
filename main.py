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
    title_episode_df = TitleEpisodeData(
        spark_session=spark_session,
        path=f'{INPUT_DATA_PATH}/title.episode.tsv',
        schema=title_episode_schema
    )

    title_episode_df.get_first_rows()
    title_episode_df.get_schema_info()
    title_episode_df.get_basic_statistics()
