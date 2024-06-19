import os
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from barn.models import metadata


@pytest.fixture(scope="session")
def engine():
    try:
        os.remove("db_test.sqlite3")
    except FileNotFoundError:
        pass
    engine = create_engine('sqlite:///db_test.sqlite3')
    # engine = create_engine('postgresql+psycopg://rds:sqlsql@host.docker.internal/barn')
    with engine.begin():
        # Base.metadata.drop_all(engine)
        # Base.metadata.create_all(engine)
        metadata.drop_all(engine)
        metadata.create_all(engine)
    yield engine
    os.remove("db_test.sqlite3")


@pytest.fixture
def session_ctx(engine):
    session_ctx = sessionmaker(engine)
    yield session_ctx


class TestLock:

    def test_1(self, session_ctx):
        assert 1 == 1
