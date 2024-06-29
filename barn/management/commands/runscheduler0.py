import signal
from threading import Event

from django.core.management.base import BaseCommand
from django.utils import autoreload

from ...scheduler import Scheduler, SimpleScheduler


class Command(BaseCommand):
    help = "Worker"

    def add_arguments(self, parser):
        parser.add_argument(
            "--with_autoreload",
            action="store_true",
            dest="use_reloader",
        )

    def handle(self, *args, **options):
        use_reloader = options["use_reloader"]
        if use_reloader:
            autoreload.run_with_reloader(self._run, **options)
        else:
            self._run(**options)

    def _run(self, **options):
        use_signals = not options["use_reloader"]

        self._stop_event = Event()
        if use_signals:
            signal.signal(signal.SIGTERM, self._sig_handler)
            signal.signal(signal.SIGINT, self._sig_handler)

        scheduler = SimpleScheduler()
        scheduler.start()

        while not self._stop_event.wait(5):
            pass

        scheduler.stop()

    def _sig_handler(self, signum, frame) -> None:
        # log.info("got signal - %s", signal.strsignal(signum))
        self._stop_event.set()