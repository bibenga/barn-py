import logging
import signal
from datetime import UTC, datetime, timedelta
from threading import Event, Thread
from uuid import uuid4

from django.db import transaction

from .models import Lock
from .signals import lock_changed

log = logging.getLogger(__name__)


class Locker:
    def __init__(
        self,
        lock_name: str,
        use_signals: bool = True,
        interval: float | int = 5,
        expiration: int | timedelta = 30,
        hostname: str = '',
    ) -> None:
        self._use_signals = use_signals
        self._lock_name = lock_name
        self._interval = interval
        self._expiration = expiration if isinstance(expiration, timedelta) \
            else timedelta(seconds=expiration)
        self._hostname = hostname or str(uuid4())
        self._is_locked = False
        self._locked_at: datetime | None = None

        self._thread: Thread | None = None
        self._stop_event = Event()
        self._stoped_event = Event()

    def run(self) -> None:
        if self._use_signals:
            signal.signal(signal.SIGTERM, self._sig_handler)
            signal.signal(signal.SIGINT, self._sig_handler)
        self._run()

    def start(self) -> None:
        self._thread = Thread(name="locker", target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._stoped_event.wait(5)

    def _sig_handler(self, signum, frame) -> None:
        log.info("got signal - %s", signal.strsignal(signum))
        self._stop_event.set()

    def _run(self) -> None:
        log.info("stated")
        try:
            self._process()
            while not self._stop_event.wait(self._interval):
                self._process()
        finally:
            if self._is_locked:
                self._release()
                self._on_released()
            self._stoped_event.set()
            log.info("finished")

    def _process(self) -> None:
        if self._is_locked:
            if not self._confirm():
                self._on_released()
        else:
            if self._acquire():
                self._on_acquired()

    @transaction.atomic
    def _acquire(self) -> bool:
        log.info("try to capture the lock %r", self._lock_name)

        locked_at = datetime.now(UTC)
        lock = Lock.objects.select_for_update().filter(name=self._lock_name).first()
        if lock is None:
            lock, created = Lock.objects.select_for_update().get_or_create(
                name=self._lock_name,
                defaults=dict(
                    locked_at=locked_at,
                    owner=self._hostname,
                ),
            )
        else:
            created = False
        if created:
            self._is_locked = True
            self._locked_at = locked_at
            log.info("the lock %r is created and acquired", self._lock_name)
        else:
            log.info("the lock state is: locked_at=%s, owner=%s, created=%r",
                     lock.locked_at, lock.owner, created)
            rotten_ts = datetime.now(UTC) - self._expiration
            if not lock.locked_at or lock.locked_at < rotten_ts:
                lock.locked_at = locked_at
                lock.owner = self._hostname
                lock.save()
                self._is_locked = True
                self._locked_at = locked_at
                log.info("the lock %r is acquired", self._lock_name)
            else:
                log.info("the lock %r cannot be acquired", self._lock_name)
        return self._is_locked

    @transaction.atomic
    def _confirm(self) -> bool:
        log.info("confirm the lock %r", self._lock_name)

        lock = Lock.objects.select_for_update().filter(name=self._lock_name).first()
        if lock is None:
            self._is_locked = False
            self._locked_at = None
            log.info("the lock was deleted by someone")
        else:
            log.info("the lock state is: locked_at=%s, owner=%s", lock.locked_at, lock.owner)
            if lock.locked_at == self._locked_at and lock.owner == self._hostname:
                self._locked_at = datetime.now(UTC)
                lock.locked_at = self._locked_at
                lock.save()
                log.info("the lock %r is confirmed", self._lock_name)
            else:
                self._is_locked = False
                self._locked_at = None
                log.info("the lock %r is released", self._lock_name)

        return self._is_locked

    @transaction.atomic
    def _release(self) -> None:
        log.info("release the lock %r", self._lock_name)
        lock = Lock.objects.select_for_update().filter(name=self._lock_name).first()
        self._is_locked = False
        self._locked_at = None
        if lock is None:
            log.info("the lock was deleted by someone")
        else:
            log.info("the lock state is: locked_at=%s, owner=%s", lock.locked_at, lock.owner)
            if lock.locked_at == self._locked_at and lock.owner == self._hostname:
                lock.locked_at = self._locked_at
                lock.owner = None
                lock.save()
                log.info("the lock %r is released", self._lock_name)
            else:
                log.info("the lock %r cannot be released", self._lock_name)

    def _on_acquired(self) -> None:
        log.debug("ON: the lock %r is acquired", self._lock_name)
        lock_changed.send(sender=self, lock=self._lock_name, locked=True)

    def _on_released(self) -> None:
        log.debug("the lock %r is released", self._lock_name)
        lock_changed.send(sender=self, lock=self._lock_name, locked=False)
