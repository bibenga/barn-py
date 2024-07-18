import logging
import signal
import threading
import time
from typing import Type

from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.db import models
from django.utils import autoreload

from ...bus import PgBus
from ...conf import Conf
from ...models import AbstractSchedule, AbstractTask
from ...scheduler import Scheduler
from ...worker import Worker

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Worker"
    scheduler_model = "barn.schedule"
    task_model = "barn.task"

    def add_arguments(self, parser):
        parser.add_argument(
            "-r",
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
            default=1,
            type=int
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

        parser.add_argument(
            "-b",
            "--bus",
            dest="bus",
            action="store_true",
        )

    def handle(self, *args, **options):
        use_reloader = options["use_reloader"]
        if use_reloader:
            log.debug("the reloader will be used")
            autoreload.run_with_reloader(self._run, **options)
        else:
            self._run(**options)

    def _run(self, **options):
        use_signals = not options["use_reloader"]
        with_scheduler = options["scheduler"]
        worker_count = options["worker"]
        with_bus = options["bus"]
        scheduler_model = options["scheduler_model"]
        task_model = options["task_model"]

        scheduler_model: Type[AbstractSchedule] = self._get_model(scheduler_model)
        task_model: Type[AbstractTask] = self._get_model(task_model)

        log.info("run with params: scheduler=%s, scheduler_model=%s, worker=%s, task_model=%s",
                 with_scheduler, scheduler_model, worker_count, task_model)

        if not with_scheduler and not worker_count and not with_bus:
            log.warning("nothing to run")
            return

        log.info("start")
        self._stop_event = threading.Event()
        if use_signals:
            for sig in [signal.SIGTERM, signal.SIGINT]:
                signal.signal(sig, self._sig_handler)

        self._bus: PgBus | None = None
        if Conf.BUS or with_bus:
            self._bus = PgBus()
            self._bus.start()

        self._scheduler: Scheduler | None = None
        if with_scheduler:
            self._scheduler = Scheduler(scheduler_model)
            self._scheduler.start()
            time.sleep(1)

        self._workers: list[Worker] = []
        if worker_count > 0:
            for i in range(worker_count):
                worker = Worker(task_model, name=f"worker-{i}")
                self._workers.append(worker)
                worker.start()
                time.sleep(1)

        while not self._stop_event.is_set():
            if not self._stop_event.wait(5):
                log.debug("I am alive")

        for worker in self._workers:
            worker.stop()

        if self._scheduler:
            self._scheduler.stop()

        if self._bus:
            self._bus.stop()

        log.info("stop")

    def _sig_handler(self, signum, frame) -> None:
        log.info("got signal - %s", signal.strsignal(signum))
        self._stop_event.set()

    def _get_model(self, name: str) -> Type[models.Model]:
        app, _, model = name.partition('.')
        clazz = ContentType.objects.get_by_natural_key(app, model).model_class()
        if clazz is None:
            raise ValueError(name)
        return clazz
