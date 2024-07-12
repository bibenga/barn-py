import asyncio
import logging
import signal
from contextlib import suppress
from functools import partial
from typing import Type

from asgiref.sync import sync_to_async
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.db import models
from django.utils import autoreload

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
        main = partial(asyncio.run, self._run(**options))
        if use_reloader:
            log.warning("the reloader will be used")
            # autoreload.run_with_reloader(self._run, **options)
            autoreload.run_with_reloader(main)
        else:
            # self._run(**options)
            main()

    async def _run(self, **options):
        use_signals = not options["use_reloader"]
        with_scheduler = options["scheduler"]
        with_worker = options["worker"]
        scheduler_model = options["scheduler_model"]
        task_model = options["task_model"]

        scheduler_model: Type[AbstractSchedule] = await self._get_model(scheduler_model)
        task_model: Type[AbstractTask] = await self._get_model(task_model)

        log.info("run with params: scheduler=%s, scheduler_model=%s, worker=%s, task_model=%s",
                 with_scheduler, scheduler_model, with_worker, task_model)

        if not with_scheduler and not with_worker:
            log.warning("nothing to run")
            return

        log.info("start")
        self._stop_event = asyncio.Event()
        if use_signals:
            for sig in [signal.SIGTERM, signal.SIGINT]:
                asyncio.get_event_loop().add_signal_handler(sig, partial(self._sig_handler, sig))

        self._scheduler: Scheduler | None = None
        if with_scheduler:
            self._scheduler = Scheduler(scheduler_model)
            await self._scheduler.start()
            await asyncio.sleep(1)

        self._worker: Worker | None = None
        if with_worker:
            self._worker = Worker(task_model)
            await self._worker.start()
            await asyncio.sleep(1)

        # await self._stop_event.wait()
        while not self._stop_event.is_set():
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), 5)
                log.debug("I am alive")

        if self._worker:
            await self._worker.stop()

        if self._scheduler:
            await self._scheduler.stop()

        log.info("stop")

    def _sig_handler(self, signum) -> None:
        log.info("got signal - %s", signal.strsignal(signum))
        self._stop_event.set()

    @sync_to_async
    def _get_model(self, name: str) -> Type[models.Model]:
        app,_, model = name.partition('.')
        clazz =  ContentType.objects.get_by_natural_key(app, model).model_class()
        if clazz is None:
            raise ValueError(name)
        return clazz
