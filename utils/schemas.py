"""Module with basic schemas for each data"""

import pyspark.sql.types as t

from utils.models import (
    TitleEpisodeModel,
    TitleRatingsModel,
    TitlePrincipalsModel,
    TitleCrewModel,
    TitleBasicsModel
)

title_episode_schema = t.StructType(fields=[
    t.StructField(name=TitleEpisodeModel.tconst, dataType=t.StringType(), nullable=False),
    t.StructField(name=TitleEpisodeModel.parent_tconst, dataType=t.StringType(), nullable=False),
    t.StructField(name=TitleEpisodeModel.season_number, dataType=t.IntegerType(), nullable=True),
    t.StructField(name=TitleEpisodeModel.episode_number, dataType=t.IntegerType(), nullable=True)
])

title_ratings_schema = t.StructType(fields=[
    t.StructField(name=TitleRatingsModel.tconst, dataType=t.StringType(), nullable=False),
    t.StructField(name=TitleRatingsModel.average_rating, dataType=t.FloatType(), nullable=True),
    t.StructField(name=TitleRatingsModel.number_votes, dataType=t.IntegerType(), nullable=True)
])

title_principals_schema = t.StructType(fields=[
    t.StructField(name=TitlePrincipalsModel.tconst, dataType=t.StringType(), nullable=False),
    t.StructField(name=TitlePrincipalsModel.ordering, dataType=t.IntegerType(), nullable=False),
    t.StructField(name=TitlePrincipalsModel.nconst, dataType=t.StringType(), nullable=False),
    t.StructField(name=TitlePrincipalsModel.category, dataType=t.StringType(), nullable=False),
    t.StructField(name=TitlePrincipalsModel.job, dataType=t.StringType(), nullable=True),
    t.StructField(name=TitlePrincipalsModel.characters, dataType=t.StringType(), nullable=True),
])

title_crew_schema = t.StructType(fields=[
    t.StructField(name=TitleCrewModel.tconst, dataType=t.StringType(), nullable=False),
    t.StructField(name=TitleCrewModel.directors, dataType=t.StringType(), nullable=False),
    t.StructField(name=TitleCrewModel.writers, dataType=t.StringType(), nullable=True),
])

title_basics_schema = t.StructType(fields=[
    t.StructField(name=TitleBasicsModel.tconst, dataType=t.StringType(), nullable=False),
    t.StructField(name=TitleBasicsModel.title_type, dataType=t.StringType(), nullable=False),
    t.StructField(name=TitleBasicsModel.primary_title, dataType=t.StringType(), nullable=False),
    t.StructField(name=TitleBasicsModel.original_title, dataType=t.StringType(), nullable=False),
    t.StructField(name=TitleBasicsModel.is_adult, dataType=t.IntegerType(), nullable=False),
    t.StructField(name=TitleBasicsModel.start_year, dataType=t.IntegerType(), nullable=True),
    t.StructField(name=TitleBasicsModel.end_year, dataType=t.IntegerType(), nullable=True),
    t.StructField(name=TitleBasicsModel.runtime_minutes, dataType=t.IntegerType(), nullable=True),
    t.StructField(name=TitleBasicsModel.genres, dataType=t.StringType(), nullable=True)
])
