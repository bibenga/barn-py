import logging
import signal
from threading import Event

from django.core.management.base import BaseCommand
from django.utils import autoreload

from ...elector import LeaderElector
from ...scheduler import Scheduler, SimpleScheduler
from ...signals import leader_changed
from ...worker import Worker

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
            "-s",
            "--scheduler",
            dest="scheduler",
            type=str,
            default="simple",
            choices=["none", "simple", "complex"]
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
            "-f",
            "--filter",
            dest="filter",
            nargs="*",
            type=str,
        )

    def handle(self, *args, **options):
        # log.info("handle: %r", options)
        use_reloader = options["use_reloader"]
        if use_reloader:
            autoreload.run_with_reloader(self._run, **options)
        else:
            self._run(**options)

    def _run(self, **options):
        use_signals = not options["use_reloader"]
        scheduler_type = options["scheduler"]
        worker_type = options["worker"]
        task_filter = options["filter"]

        if scheduler_type == "none" and worker_type == "none":
            log.warning("nothing to run")
            return

        log.info("start")
        self._stop_event = Event()
        if use_signals:
            signal.signal(signal.SIGTERM, self._sig_handler)
            signal.signal(signal.SIGINT, self._sig_handler)

        self._scheduler: SimpleScheduler | Scheduler | None = None
        if scheduler_type == "simple":
            self._scheduler = SimpleScheduler()
            self._scheduler.start()
        elif scheduler_type == "complex":
            self._scheduler = Scheduler()

        self._worker: Worker | None = None
        if worker_type == "simple":
            self._worker = Worker(task_filter=task_filter)
            self._worker.start()

        self._elector: LeaderElector | None = None
        if scheduler_type == "complex":
            self._elector = LeaderElector()
            self._elector.start()
            leader_changed.connect(self._leader_changed)

        while not self._stop_event.wait(5):
            log.debug("I am alive")

        if self._elector:
            self._elector.stop()

        if self._worker:
            self._worker.stop()

        if self._scheduler:
            self._scheduler.stop()

        log.info("stop")

    def _sig_handler(self, signum, frame) -> None:
        log.info("got signal - %s", signal.strsignal(signum))
        self._stop_event.set()

    def _leader_changed(self, is_leader: bool, **_kwargs) -> None:
        if is_leader:
            self._scheduler.start()
        else:
            self._scheduler.stop()
