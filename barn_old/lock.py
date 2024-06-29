import logging
import time
from datetime import datetime, timedelta, UTC
from typing import Optional
from uuid import uuid4

from sqlalchemy import select, update, insert, Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from barn_old.models import Lock, lock_table

log = logging.getLogger(__name__)


class LockManager:
    def __init__(
        self,
        engine: Engine,
        session_ctx: sessionmaker[Session],
        name: str = "barn",
        interval: int = 5,
        expiration: int = 30,
        hostname: str = '',
    ) -> None:
        self._engine = engine
        self._session_ctx = session_ctx
        self._hostname = hostname or str(uuid4())
        self._name = name
        self._interval = interval
        self._expiration = timedelta(seconds=expiration)
        self._is_locked = False
        self._locked_at: Optional[datetime] = None

    def run(self) -> None:
        log.info("> %s", self._hostname)

        self._create()

        while True:
            if self._is_locked:
                if self._update():
                    pass
                else:
                    self._on_released()
            else:
                if self._try_capture():
                    self._on_captured()
                else:
                    pass
            time.sleep(self._interval)

        self._release()

    def _create(self) -> None:
        log.info("checking the lock %r existence...", self._name)
        try:
            with self._engine.connect() as c:
                res = c.execute(select(lock_table).where(lock_table.c.name == self._name)).one_or_none()
                if res is None:
                    c.execute(insert(lock_table).values(name=self._name))
                    c.commit()

            # with self._session_ctx() as session:
            #     l = session.get(Lock, self._name)
            #     if l is None:
            #         log.info("the lock %r is not found, try to create it...", self._name)
            #         stmt = insert(Lock).values(
            #             name=self._name,
            #         )
            #         session.execute(stmt)
            #         session.commit()
            log.info("the lock %r is created", self._name)
        except IntegrityError:
            log.info("the lock %r exists", self._name)
            pass

    def _try_capture(self) -> bool:
        log.info("try to capture the lock %r", self._name)

        with self._engine.connect() as c:
            locked_at = datetime.now(UTC)
            rotten_ts = datetime.now(UTC) - self._expiration
            stmt = update(lock_table).where(
                lock_table.c.name == self._name,
                lock_table.c.locked_at.is_(None) | (lock_table.c.locked_at < rotten_ts),
            ).values(
                locked_at=locked_at,
                locked_by=self._hostname,
            )
            res = c.execute(stmt)
            if res.rowcount == 1:
                log.info("the lock %r is captured", self._name)
                self._is_locked = True
                self._locked_at = locked_at
            c.commit()

        # with self._session_ctx() as session:
        #     locked_at = datetime.now(UTC)
        #     rotten_ts = datetime.now(UTC) - self._expiration
        #     stmt = update(Lock).where(
        #         Lock.name == self._name,
        #         Lock.locked_at.is_(None) | (Lock.locked_at < rotten_ts),
        #     ).values(
        #         locked_at=locked_at,
        #         locked_by=self._hostname,
        #     )
        #     res = session.execute(stmt)
        #     if res.rowcount == 1:
        #         log.info("the lock %r is captured", self._name)
        #         self._is_locked = True
        #         self._locked_at = locked_at
        #     session.commit()
        return self._is_locked

    def _update(self) -> bool:
        log.info("update the lock %r", self._name)

        with self._engine.connect() as c:
            locked_at = datetime.now(UTC)
            stmt = update(lock_table).where(
                lock_table.c.name == self._name,
                lock_table.c.locked_at == self._locked_at,
                lock_table.c.locked_by == self._hostname,
            ).values(
                locked_at=locked_at,
            )
            res = c.execute(stmt)
            if res.rowcount == 1:
                log.info("the lock %r is still captured", self._name)
                self._is_locked = True
                self._locked_at = locked_at
            else:
                log.info("the lock %r is released", self._name)
                self._is_locked = False
                self._locked_at = None
            c.commit()

        # with self._session_ctx() as session:
        #     locked_at = datetime.now(UTC)
        #     stmt = update(Lock).where(
        #         Lock.name == self._name,
        #         Lock.locked_at == self._locked_at,
        #         Lock.locked_by == self._hostname,
        #     ).values(
        #         locked_at=locked_at,
        #     )
        #     res = session.execute(stmt)
        #     if res.rowcount == 1:
        #         log.info("the lock %r is still captured", self._name)
        #         self._is_locked = True
        #         self._locked_at = locked_at
        #     else:
        #         log.info("the lock %r is released", self._name)
        #         self._is_locked = False
        #         self._locked_at = None
        #     session.commit()
        return self._is_locked

    def _release(self) -> None:
        log.info("release the lock %r", self._name)
        with self._engine.connect() as c:
            stmt = update(lock_table).where(
                lock_table.c.name == self._name,
                lock_table.c.locked_at == self._locked_at,
                lock_table.c.locked_by == self._hostname,
            ).values(
                locked_at=None,
                locked_by=None,
            )
            res = c.execute(stmt)
            self._is_locked = False
            self._locked_at = None
            if res.rowcount == 1:
                log.info("the lock %r is released", self._name)
            else:
                log.info("the lock %r cannot be released", self._name)
            c.commit()

        # with self._session_ctx() as session:
        #     stmt = update(Lock).where(
        #         Lock.name == self._name,
        #         Lock.locked_at == self._locked_at,
        #         Lock.locked_by == self._hostname,
        #     ).values(
        #         locked_at=None,
        #         locked_by=None,
        #     )
        #     res = session.execute(stmt)
        #     self._is_locked = False
        #     self._locked_at = None
        #     if res.rowcount == 1:
        #         log.info("the lock %r is released", self._name)
        #     else:
        #         log.info("the lock %r cannot be released", self._name)
        #     session.commit()

    def _on_captured(self) -> None:
        log.info("ON: the lock %r is captured", self._name)

    def _on_released(self) -> None:
        log.info("ON: the lock %r is released", self._name)
