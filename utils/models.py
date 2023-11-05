"""Module with basic models for each data"""

class TitleAkasModel:
    """Fields for the `title.akas` data"""

class TitleBasicsModel:
    """Fields for the `title.basics` data"""

class TitleCrewModel:
    """Fields for the `title.crew` data"""

class TitleEpisodeModel:
    """Fields for the `title.episode` data"""
    tconst = 'tconst'
    parent_tconst = 'parentTconst'
    season_number = 'seasonNumber'
    episode_number = 'episodeNumber'

class TitlePrincipalsModel:
    """Fields for the `title.principals` data"""

class TitleRatingsModel:
    """Fields for the `title.ratings` data"""
    tconst = 'tconst'
    average_rating = 'averageRating'
    number_votes = 'numVotes'

class NameBasicsModel:
    """Fields for the `name.basics` data"""
