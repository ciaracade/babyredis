"""Opt-in pytest fixtures for using babyredis as a Redis test double.

Enable in a test module or conftest.py with:

    pytest_plugins = ["babyredis.testing"]

Then any test can take ``babyredis_client`` (bytes responses, like
redis-py's default) or ``babyredis_client_decoded`` (str responses).
"""

import pytest

from babyredis.client import BabyRedis


@pytest.fixture
def babyredis_client(tmp_path):
    client = BabyRedis(str(tmp_path / "babyredis-test.db"))
    yield client
    client.close()


@pytest.fixture
def babyredis_client_decoded(tmp_path):
    client = BabyRedis(str(tmp_path / "babyredis-test.db"),
                       decode_responses=True)
    yield client
    client.close()
