import logging
import signal
from threading import Event
from django.core.management.base import BaseCommand
from django.utils import autoreload

from ...worker import Worker
from ...scheduler import SimpleScheduler

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Worker"

    def add_arguments(self, parser):
        parser.add_argument(
            "--with-autoreload",
            dest="use_reloader",
            action="store_true",
        )

        parser.add_argument(
            "-w",
            "--worker",
            dest="worker",
            type=str,
            default="simple",
            choices=["none", "simple"]
        )

        parser.add_argument(
            "-s",
            "--scheduler",
            dest="scheduler",
            type=str,
            default="simple",
            choices=["none", "simple", "complex"]
        )

        parser.add_argument(
            "-l",
            "--lock-name",
            dest="lock_name",
            type=str,
            default="barn",
        )

        parser.add_argument(
            "-f",
            "--filter",
            dest="filter",
            nargs="*",
            type=str,
        )

        parser.add_argument(
            "-i",
            "--interval",
            dest="interval",
            type=int,
        )

    def handle(self, *args, **options):
        # log.info("handle: %r", options)
        use_reloader = options["use_reloader"]
        if use_reloader:
            autoreload.run_with_reloader(self._run, **options)
        else:
            self._run(**options)

    def _run(self, use_reloader: bool, **options):
        use_signals = not options["use_reloader"]

        if use_signals:
            signal.signal(signal.SIGTERM, self._sig_handler)
            signal.signal(signal.SIGINT, self._sig_handler)

        self._stop_event = Event()
        log.info("start")

        worker = Worker()
        worker.start()

        scheduler = SimpleScheduler()
        scheduler.start()

        while not self._stop_event.wait(5):
            log.debug("I am alive")

        scheduler.stop()
        worker.stop()
        log.info("stop")

    def _sig_handler(self, signum, frame) -> None:
        log.info("got signal - %s", signal.strsignal(signum))
        self._stop_event.set()
