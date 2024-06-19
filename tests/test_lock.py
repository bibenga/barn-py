import os
from typing import Generator

import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from barn.models import metadata


@pytest.fixture(scope="session")
def engine() -> Generator[Engine, None, None]:
    try:
        os.remove("db_test.sqlite3")
    except FileNotFoundError:
        pass
    engine = create_engine('sqlite:///db_test.sqlite3')
    # engine = create_engine('postgresql+psycopg://rds:sqlsql@host.docker.internal/barn')
    # with engine.begin():
    #     # Base.metadata.drop_all(engine)
    #     # Base.metadata.create_all(engine)
    #     metadata.drop_all(engine)
    #     metadata.create_all(engine)
    yield engine
    os.remove("db_test.sqlite3")


@pytest.fixture
def session_ctx(engine) -> Generator[sessionmaker[Session], None, None]:
    session_ctx = sessionmaker(engine)
    yield session_ctx


@pytest.fixture(scope="function")
def session(engine: Engine, session_ctx: sessionmaker[Session]) -> Generator[Session, None, None]:
    metadata.create_all(engine)
    try:
        with session_ctx() as session:
            yield session
    finally:
        metadata.drop_all(engine)


class TestLock:

    def test_1(self, session: Session) -> None:
        assert 1 == 1
