import logging
import signal
from threading import Event
from django.core.management.base import BaseCommand
from django.utils import autoreload

from barn2.lock.elector import LeaderElector

from ...worker import Worker
from ...scheduler import SimpleScheduler, Scheduler

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

        self._stop_event = Event()
        log.info("start")

        if use_signals:
            signal.signal(signal.SIGTERM, self._sig_handler)
            signal.signal(signal.SIGINT, self._sig_handler)

        scheduler: SimpleScheduler | Scheduler | None = None
        if scheduler_type == "simple":
            scheduler = SimpleScheduler()
            scheduler.start()
        elif scheduler_type == "complex":
            scheduler = Scheduler()

        worker: Worker | None = None
        if worker_type == "simple":
            worker = Worker(task_filter=task_filter)
            worker.start()

        elector: LeaderElector | None = None
        if scheduler_type == "complex":
            elector = LeaderElector()
            elector.start()

        while not self._stop_event.wait(5):
            log.debug("I am alive")

        if elector:
            elector.stop()

        if worker:
            worker.stop()

        if scheduler:
            scheduler.stop()

        log.info("stop")

    def _sig_handler(self, signum, frame) -> None:
        log.info("got signal - %s", signal.strsignal(signum))
        self._stop_event.set()
