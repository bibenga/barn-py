import logging
import signal
import threading
import time
from collections import Counter
from typing import Type

from django.apps import apps
from django.core.management.base import BaseCommand
from django.db import models
from django.utils import autoreload

from ...bus import PgBus
from ...conf import Conf
from ...models import AbstractSchedule, AbstractTask
from ...scheduler import Scheduler
from ...signals import post_schedule_execute, post_task_execute
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

        self._stats_lock = threading.Lock()
        self._stats = Counter()

        log.info("start")
        self._stop_event = threading.Event()
        if use_signals:
            for sig in [signal.SIGTERM, signal.SIGINT]:
                signal.signal(sig, self._sig_handler)

        self._scheduler: Scheduler | None = None
        if with_scheduler:
            post_schedule_execute.connect(self._on_schedule_executed)
            self._scheduler = Scheduler(scheduler_model)
            self._scheduler.start()
            time.sleep(0.2)

        self._workers: list[Worker] = []
        if worker_count > 0:
            post_task_execute.connect(self._on_task_executed)
            for i in range(worker_count):
                worker = Worker(task_model, name=f"worker-{i}")
                self._workers.append(worker)
                worker.start()
                time.sleep(0.2)

        self._bus: PgBus | None = None
        if Conf.BUS_ENABLED or with_bus:
            bus_models: list[Type[AbstractSchedule] | Type[AbstractTask]] = []
            if with_scheduler:
                bus_models.append(scheduler_model)
            if worker_count > 0:
                bus_models.append(task_model)
            self._bus = PgBus(*bus_models)
            self._bus.start()

        with self._stats_lock:
            prev_stats = self._stats.copy()

        timeout = 1
        while not self._stop_event.is_set():
            if not self._stop_event.wait(timeout):
                if self.is_alive():
                    with self._stats_lock:
                        stats = self._stats.copy()
                    rps: dict[str, float] = {
                        k: v / timeout
                        for k, v in (stats - prev_stats).items()
                    }
                    prev_stats = stats
                    if rps:
                        log.info("rps: %s", rps)
                    else:
                        log.debug("I am alive")
                else:
                    break

        if self._bus:
            self._bus.stop()

        for worker in self._workers:
            worker.stop()

        if self._scheduler:
            self._scheduler.stop()

        log.info("stop")

    def _sig_handler(self, signum, frame) -> None:
        log.info("got signal - %s", signal.strsignal(signum))
        self._stop_event.set()

    def _get_model(self, name: str) -> Type[models.Model]:
        app_label, _, model_name = name.partition('.')
        return apps.get_model(app_label, model_name)

    def is_alive(self) -> bool:
        if self._bus and not self._bus.is_alive():
            log.error("the og_bus is died")
            return False
        if self._scheduler and not self._scheduler.is_alive():
            log.error("the scheduler is died")
            return False
        if self._workers:
            for worker in self._workers:
                if not worker.is_alive():
                    log.error("the worker %r is died", worker.name)
                    return False
        return True

    def _on_schedule_executed(self, sender, schedule: AbstractSchedule, **kwargs) -> None:
        model = f"{schedule._meta.app_label}.{schedule._meta.model_name}"
        with self._stats_lock:
            self._stats[model] += 1

    def _on_task_executed(self, sender, task: AbstractTask, **kwargs) -> None:
        model = f"{task._meta.app_label}.{task._meta.model_name}"
        with self._stats_lock:
            self._stats[model] += 1
