import logging
import time
from datetime import datetime, timedelta, UTC
from uuid import uuid4

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from barn.models import Lock

log = logging.getLogger(__name__)


class LockManager:
    def __init__(
        self,
        session_ctx: sessionmaker[Session],
        name: str = "barn",
        interval: int = 5,
        expiration: int = 30,
        hostname: str = '',
    ) -> None:
        self._session_ctx = session_ctx
        self._hostname = hostname or str(uuid4())
        self._name = name
        self._interval = interval
        self._expiration = timedelta(seconds=expiration)
        self._is_locked = False

    def run(self) -> None:
        log.info("> %s", self._hostname)

        log.info("checking the lock %r existence...", self._name)
        with self._session_ctx() as session:
            l = session.get(Lock, self._name)
            if l is None:
                log.info("the lock %r is not found, try to create it...", self._name)
                stmt = insert(Lock).values(
                    name=self._name,
                    locked_at=datetime.now(UTC) - timedelta(days=300),
                    locked_by=""
                ).on_conflict_do_nothing(
                    index_elements=(Lock.name,),
                ).returning(Lock)
                res = session.execute(stmt)
                l = res.one().Lock
                if l.locked_by == "":
                    log.info("the lock %r is created", self._name)
                    session.commit()
                else:
                    log.info("the lock %r was created by someone", self._name)
            else:
                log.info("the lock %r already exists", self._name)

        while True:
            with self._session_ctx() as session:
                l = session.get_one(Lock, self._name)
                log.info("the lock %r is locked by %r at %s", self._name, l.locked_by, l.locked_at)
                if self._is_locked:
                    if l.locked_by == self._name:
                        stmt = update(Lock).where(
                            Lock.name == self._name,
                            Lock.locked_by == self._name,
                            Lock.locked_at == l.locked_at,
                        ).values(
                            locked_at=datetime.now(UTC),
                        )
                        res = session.execute(stmt)
                        if res.rowcount != 1:
                            log.warning("the lock %r was captured unexpectedly by someone", self._name)
                            self._is_locked = False
                            self._on_released()
                        else:
                            log.info("the lock %r is still captured", self._name)
                            session.commit()
                    else:
                        log.warning("the lock %r was captured by someone", self._name)
                        self._is_locked = False
                        self._on_released()
                elif (datetime.now(UTC)-l.locked_at) > self._expiration:
                    log.info("the lock %r is rotten", self._name)
                    stmt = update(Lock).where(
                        Lock.name == self._name,
                        Lock.locked_by == l.locked_by,
                        Lock.locked_at == l.locked_at,
                    ).values(
                        locked_by=self._name,
                        locked_at=datetime.now(UTC),
                    )
                    res = session.execute(stmt)
                    if res.rowcount == 1:
                        log.info("the lock %r is captured", self._name)
                        session.commit()
                        self._is_locked = True
                        self._on_captured()
            time.sleep(self._interval)

    def _create(self) -> None:
        pass

    def _update(self) -> bool:
        return True

    def _try_capture(self) -> bool:
        return False

    def _on_captured(self) -> None:
        log.info("lock %r is captured", self._name)

    def _on_released(self) -> None:
        log.info("lock %r is released", self._name)
