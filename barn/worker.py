import asyncio
import logging
import traceback
from contextlib import suppress
from datetime import timedelta
from random import random
from typing import Type

import asgiref.local
from asgiref.sync import sync_to_async
from django.db import transaction
from django.utils import timezone

from .conf import Conf
from .models import AbstractTask, Task, TaskStatus
from .signals import post_task_execute, pre_task_execute

log = logging.getLogger(__name__)

_current_task = asgiref.local.Local()


def get_current_taskt() -> AbstractTask | None:
    return getattr(_current_task, "value", None)


class Worker:
    def __init__(
        self,
        model: Type[AbstractTask] | None = None,
    ) -> None:
        self._model = model or Task
        self._interval: float = Conf.TASL_POLL_INTERVAL.total_seconds()
        self._ttl: timedelta | None = Conf.TASK_FINISHED_TTL

        self._stop_event = asyncio.Event()
        self._thread: asyncio.Task | None = None

    async def start(self) -> None:
        self._stop_event.clear()
        self._thread = asyncio.create_task(self._run(), name="worker")

    async def stop(self) -> None:
        if self._thread and not self._stop_event.is_set():
            self._stop_event.set()
            # self._thread.cancel()
            await self._thread

    async def run(self) -> None:
        await self._run()

    async def _run(self) -> None:
        log.info("stated")
        try:
            while not self._stop_event.is_set():
                await self._process()
                if self._ttl:
                    await self._delete_old()
                await self._sleep()
        # except asyncio.CancelledError:
        #     log.info("canceled", exc_info=True)
        finally:
            log.info("finished")

    async def _sleep(self) -> None:
        jitter = self._interval / 10
        timeout = self._interval + (jitter * random() - jitter / 2)
        log.debug("sleep for %.2fs", timeout)
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), 5)

    async def _process(self) -> None:
        while not self._stop_event.is_set():
            processed = await self._process_next()
            if not processed:
                break

    @sync_to_async
    @transaction.atomic
    def _process_next(self) -> bool:
        task_qs = self._model.objects.filter(
            run_at__lt=timezone.now(),
            status=TaskStatus.QUEUED,
        ).order_by("run_at", "id")
        task = task_qs.select_for_update(skip_locked=True).first()
        if not task:
            log.debug("no pending tasks")
            return False
        self._process_one(task)
        return True

    @transaction.atomic
    def sync_call_task(self, task: AbstractTask) -> None:
        task = self._model.objects.select_for_update().get(pk=task.pk)
        if task.status != TaskStatus.QUEUED:
            raise ValueError("The task already processed. Is worker running?")
        self._process_one(task)

    def _process_one(self, task: AbstractTask) -> None:
        _current_task.value = task
        log.info("process the task %s task", task)

        task.started_at = timezone.now()
        try:
            pre_task_execute.send(sender=self, task=task)

            task.process()

            task.status = TaskStatus.DONE
            task.error = None
            task.finished_at = timezone.now()

            post_task_execute.send(sender=self, task=task, exc=None)
            task.save()
            log.info("the task %s is processed with success in %s", task.pk,
                     task.finished_at - task.started_at)

        except Exception as exc:
            task.status = TaskStatus.FAILED
            task.error = "\n".join(traceback.format_exception(exc))
            task.finished_at = timezone.now()

            post_task_execute.send(sender=self, task=task, exc=exc)
            task.save()
            log.info("the task %s is processed with error in %s", task.pk,
                     task.finished_at - task.started_at, exc_info=True)

        finally:
            del _current_task.value

    @sync_to_async
    @transaction.atomic
    def _delete_old(self) -> None:
        moment = timezone.now() - self._ttl
        task_qs = self._model.objects.filter(
            status__in=[TaskStatus.DONE, TaskStatus.FAILED],
            run_at__lt=moment
        )
        deleted, _ = task_qs.delete()
        log.log(
            logging.DEBUG if deleted == 0 else logging.INFO,
            "deleted %d finished tasks older than %s",
            deleted, moment
        )
