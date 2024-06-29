import signal
from threading import Event

from django.core.management.base import BaseCommand
from django.utils import autoreload

from ...worker import Worker


class Command(BaseCommand):
    help = "Worker"

    def add_arguments(self, parser):
        parser.add_argument(
            "--with_autoreload",
            action="store_true",
            dest="use_reloader",
        )
        parser.add_argument(
            "--interval",
            type=int,
            default=5,
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

        worker = Worker()
        worker.start()

        while not self._stop_event.wait(5):
            pass

        worker.stop()

    def _sig_handler(self, signum, frame) -> None:
        # log.info("got signal - %s", signal.strsignal(signum))
        self._stop_event.set()
