import logging
import threading
import traceback
from datetime import timedelta
from random import random
from typing import Type

import asgiref.local
from django.db import transaction
from django.utils import timezone

from .conf import Conf
from .models import AbstractTask, Task, TaskStatus
from .signals import post_task_execute, pre_task_execute, remote_post_save

log = logging.getLogger(__name__)

_current_task = asgiref.local.Local()


def get_current_taskt() -> AbstractTask | None:
    return getattr(_current_task, "value", None)


class Worker:
    def __init__(
        self,
        model: Type[AbstractTask] | None = None,
        name: str | None = None,
    ) -> None:
        self._model = model or Task
        self._interval: float = Conf.TASL_POLL_INTERVAL.total_seconds()
        self._ttl: timedelta | None = Conf.TASK_FINISHED_TTL
        self._name = name or "worker"

        self._stop_event = threading.Event()
        self._wakeup_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def name(self) -> str:
        return self._name

    def start(self) -> None:
        self._stop_event.clear()
        self._wakeup_event.clear()
        self._thread = threading.Thread(target=self.run, name=self._name)
        self._thread.start()
        remote_post_save.connect(self._on_remote_post_save)

    def stop(self) -> None:
        remote_post_save.disconnect(self._on_remote_post_save)
        if self._thread and not self._stop_event.is_set():
            self._stop_event.set()
            self._wakeup_event.set()
            self._thread.join(5)

    def wakeup(self) -> None:
        self._wakeup_event.set()

    def is_alive(self) -> bool:
        return self._thread and self._thread.is_alive()

    def _on_remote_post_save(self, sender, **kwargs):
        log.debug("_on_remote_post_save: %s", kwargs)
        model = kwargs["model"]
        if self._model == model or issubclass(self._model, model):
            self._wakeup_event.set()

    def run(self) -> None:
        log.info("stated")
        try:
            self._run()
        except:
            log.fatal("failed")
            raise
        finally:
            log.info("finished")

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self._process()
            if self._ttl:
                self._delete_old()
            self._sleep()

    def _sleep(self) -> None:
        jitter = self._interval / 10
        timeout = self._interval + (jitter * random() - jitter / 2)
        log.debug("sleep for %.2fs", timeout)
        self._wakeup_event.wait(timeout)
        self._wakeup_event.clear()

    def _process(self) -> None:
        cnt = 0
        while not self._stop_event.is_set():
            processed = self._process_next()
            if not processed:
                break
            cnt += 1
        if cnt == 0:
            log.debug("no pending tasks")
        else:
            log.debug("processed %d tasks", cnt)

    @transaction.atomic
    def _process_next(self) -> bool:
        task_qs = self._model.objects.filter(
            run_at__lt=timezone.now(),
            status=TaskStatus.QUEUED,
        ).order_by("run_at", "id")
        task = task_qs.select_for_update(skip_locked=True).first()
        if not task:
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
