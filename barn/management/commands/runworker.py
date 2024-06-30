import logging
import signal
import time
from threading import Event

from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.utils import autoreload

from ...scheduler import Scheduler
from ...worker import Worker

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Worker"
    scheduler_model = "barn.schedule"
    task_model = "barn.task"

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
            action="store_true",
        )

        parser.add_argument(
            "-sm",
            "--scheduler-model",
            dest="scheduler_model",
            default=self.scheduler_model,
        )

        parser.add_argument(
            "-w",
            "--worker",
            dest="worker",
            action="store_true",
        )

        parser.add_argument(
            "-tm",
            "--task-model",
            dest="task_model",
            default=self.task_model,
        )

        parser.add_argument(
            "-d",
            "--delete",
            dest="delete",
            action="store_true",
        )

    def handle(self, *args, **options):
        # log.info("handle: %r", options)
        # return
        use_reloader = options["use_reloader"]
        if use_reloader:
            autoreload.run_with_reloader(self._run, **options)
        else:
            self._run(**options)

    def _run(self, **options):
        use_signals = not options["use_reloader"]
        with_scheduler = options["scheduler"]
        with_worker = options["worker"]
        scheduler_model = options["scheduler_model"]
        task_model = options["task_model"]

        scheduler_app,_, scheduler_model = scheduler_model.partition('.')
        scheduler_model = ContentType.objects.get_by_natural_key(scheduler_app, scheduler_model).model_class()

        task_app,_, task_model = task_model.partition('.')
        task_model = ContentType.objects.get_by_natural_key(task_app, task_model).model_class()

        log.info("run with params: scheduler=%s, scheduler_model=%s, worker=%s, task_model=%s",
                 with_scheduler, scheduler_model, with_worker, task_model)

        if not with_scheduler and not with_worker:
            log.warning("nothing to run")
            return

        log.info("start")
        self._stop_event = Event()
        if use_signals:
            signal.signal(signal.SIGTERM, self._sig_handler)
            signal.signal(signal.SIGINT, self._sig_handler)

        self._scheduler: Scheduler | None = None
        if with_scheduler:
            self._scheduler = Scheduler(scheduler_model)
            self._scheduler.start()
            time.sleep(1)

        self._worker: Worker | None = None
        if with_worker:
            self._worker = Worker(task_model)
            self._worker.start()
            time.sleep(1)

        while not self._stop_event.wait(5):
            log.debug("I am alive")

        if self._worker:
            self._worker.stop()

        if self._scheduler:
            self._scheduler.stop()

        log.info("stop")

    def _sig_handler(self, signum, frame) -> None:
        log.info("got signal - %s", signal.strsignal(signum))
        self._stop_event.set()
