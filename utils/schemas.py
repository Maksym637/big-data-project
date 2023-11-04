import pyspark.sql.types as t

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
