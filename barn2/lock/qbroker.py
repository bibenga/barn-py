import logging
import signal
from datetime import UTC, datetime
from threading import Event, Thread

from django.db import transaction

from .models import Queue
from .signals import got_message

log = logging.getLogger(__name__)


class QBroker:
    def __init__(
        self,
        use_signals: bool = True,
        interval: float | int = 5
    ) -> None:
        self._use_signals = use_signals
        self._interval = interval
        self._thread: Thread | None = None
        self._stop_event = Event()
        self._stoped_event = Event()

    def run(self) -> None:
        if self._use_signals:
            signal.signal(signal.SIGTERM, self._sig_handler)
            signal.signal(signal.SIGINT, self._sig_handler)
        self._run()

    def _sig_handler(self, signum, frame) -> None:
        log.info("got signal - %s", signal.strsignal(signum))
        self._stop_event.set()

    def start(self) -> None:
        self._thread = Thread(name="qbroker", target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._stoped_event.wait(5)

    def _run(self) -> None:
        log.info("stated")
        try:
            self._process()
            while not self._stop_event.wait(self._interval):
                self._process()
        finally:
            self._stoped_event.set()
            log.info("finished")

    def _process(self) -> None:
        while not self._stop_event.is_set():
            with transaction.atomic():
                message_qs = Queue.objects.order_by("created", "id")
                message = message_qs.select_for_update(skip_locked=True).first()
                if not message:
                    log.info("no pending message is found")
                    break
                log.info("found a message %s", message.pk)

                started_at = datetime.now(UTC)
                got_message.send(sender=self, message=message)
                finished_at = datetime.now(UTC)
                log.info("the task %s is success in %s", message.pk, finished_at - started_at)
                message.delete()
