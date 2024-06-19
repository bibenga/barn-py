import os
from typing import Generator

import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from barn.models import metadata


@pytest.fixture(scope="session")
def engine() -> Generator[Engine, None, None]:
    engine = create_engine('sqlite:///:memory:')
    # engine = create_engine('postgresql+psycopg://rds:sqlsql@host.docker.internal/barn')
    # with engine.begin():
    #     # Base.metadata.drop_all(engine)
    #     # Base.metadata.create_all(engine)
    #     metadata.drop_all(engine)
    #     metadata.create_all(engine)
    metadata.create_all(engine)
    yield engine


@pytest.fixture
def session_ctx(engine) -> Generator[sessionmaker[Session], None, None]:
    metadata.create_all(engine)
    session_ctx = sessionmaker(engine)
    yield session_ctx


@pytest.fixture(scope="function")
def session(engine: Engine, session_ctx: sessionmaker[Session]) -> Generator[Session, None, None]:
    with session_ctx() as session:
        yield session

