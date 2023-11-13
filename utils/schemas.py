"""Module with basic schemas for each data"""

import pyspark.sql.types as t
from utils.models import TitlePrincipalsModel

title_episode_schema = t.StructType(fields=[
    t.StructField(name='tconst', dataType=t.StringType(), nullable=False),
    t.StructField(name='parentTconst', dataType=t.StringType(), nullable=False),
    t.StructField(name='seasonNumber', dataType=t.IntegerType(), nullable=True),
    t.StructField(name='episodeNumber', dataType=t.IntegerType(), nullable=True)
])

title_ratings_schema = t.StructType(fields=[
    t.StructField(name='tconst', dataType=t.StringType(), nullable=False),
    t.StructField(name='averageRating', dataType=t.FloatType(), nullable=True),
    t.StructField(name='numVotes', dataType=t.IntegerType(), nullable=True)
])

title_principals_schema = t.StructType(fields=[
    t.StructField(name=TitlePrincipalsModel.tconst, dataType=t.StringType(), nullable=False),
    t.StructField(name=TitlePrincipalsModel.ordering, dataType=t.IntegerType(), nullable=False),
    t.StructField(name=TitlePrincipalsModel.nconst, dataType=t.StringType(), nullable=False),
    t.StructField(name=TitlePrincipalsModel.category, dataType=t.StringType(), nullable=False),
    t.StructField(name=TitlePrincipalsModel.job, dataType=t.StringType(), nullable=True),
    t.StructField(name=TitlePrincipalsModel.characters, dataType=t.StringType(), nullable=True),
])