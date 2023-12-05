"""Module with tests for the the `title.episode` jobs"""

from utils.constants import INPUT_DATA_PATH
from utils.models import TitleEpisodeModel
from utils.schemas import title_episode_schema

from jobs.title_episode_job import TitleEpisodeData

from tests.base_test_job import BaseJobTest


class TitleEpisodeJobTest(BaseJobTest):
    """Tests for the `title.episode` business questions"""

    def setUp(self) -> None:
        self.title_episode_df = TitleEpisodeData(
            spark_session=self.spark_session,
            path=INPUT_DATA_PATH,
            schema=title_episode_schema
        )

    def test_get_title_episodes_with_provided_season_number(self) -> None:
        actual_result = (
            self.title_episode_df
                .get_title_episodes_with_provided_season_number()
                .first()[TitleEpisodeModel.season_number]
        )
        expected_result = 6

        self.assertEqual(
            first=actual_result,
            second=expected_result,
            msg=f'Expected: {expected_result}, but got: {actual_result}'
        )

    def test_get_first_title_episodes_in_interval(self) -> None:
        pass

    def test_get_first_latest_title_episodes(self) -> None:
        pass
