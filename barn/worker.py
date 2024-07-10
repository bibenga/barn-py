import logging
import traceback
from datetime import datetime
from threading import Event, Thread
from typing import Type

from croniter import croniter
from django.db import transaction
from django.utils import timezone

from .conf import Conf
from .models import AbstractTask, Task

log = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        model: Type[AbstractTask] | None,
    ) -> None:
        self._model = model or Task
        self._cron = Conf.TASK_POLL_CRON
        self._ttl = Conf.TASK_FINISHED_TTL
        self._thread: Thread | None = None
        self._stop_event = Event()

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = Thread(name="worker", target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if not self._stop_event.is_set():
            self._stop_event.set()
            self._thread.join(60)

    def _run(self) -> None:
        log.info("stated")
        try:
            while not self._stop_event.is_set():
                self._process()
                self._delete_old()
                self._sleep()
        finally:
            log.info("finished")

    def _sleep(self) -> None:
        now = timezone.now()
        iter = croniter(self._cron, now)
        next_run_at = iter.get_next(datetime)
        sleep_seconds = next_run_at - now
        log.info("sleep for %s", sleep_seconds)
        self._stop_event.wait(sleep_seconds.total_seconds())

    def _process(self) -> None:
        while not self._stop_event.is_set():
            with transaction.atomic():
                task_qs = self._model.objects.filter(
                    run_at__lt=timezone.now(),
                    is_processed=False,
                ).order_by("run_at", "id")
                task = task_qs.select_for_update(skip_locked=True).first()
                if not task:
                    log.info("no pending task is found")
                    break
                log.info("found a task %s", task.pk)

                self._call_task(task)

    def _delete_old(self) -> None:
        if self._ttl is None:
            return
        with transaction.atomic():
            moment = timezone.now() - self._ttl
            task_qs = self._model.objects.filter(
                is_processed=True,
                run_at__lt=moment
            )
            deleted, _ = task_qs.delete()
            log.log(
                logging.DEBUG if deleted == 0 else logging.INFO,
                "deleted %d tasks older than %s",
                deleted, moment
            )

    @transaction.atomic
    def sync_call_task(self, task: AbstractTask) -> None:
        task = self._model.objects.select_for_update().get(pk=task.pk)
        if task.is_processed:
            raise ValueError("The task already processed. Is worker running?")
        self._call_task(task)

    def _call_task(self, task: AbstractTask) -> None:
        log.info("process the task %s task", task)
        task.started_at = timezone.now()
        try:
            with transaction.atomic():
                task.process()
        except Exception as exc:
            log.info("the task %s is failed", task.pk, exc_info=True)
            task.is_success = False
            task.error = "\n".join(traceback.format_exception(exc))
            # post_execute.send(sender=self, task=task, success=False, result=None, exc=exc)
        else:
            log.info("the task %s is finished", task.pk)
            task.is_success = True
            # post_execute.send(sender=self, task=task, success=True, result=result, exc=None)
        finally:
            task.is_processed = True
            task.finished_at = timezone.now()
            # task.save(update_fields=["is_processed", "started_at",
            #                          "finished_at", "is_success", "result", "error"])
            task.save()

            log.info("the task %s is processed in %s", task.pk,
                     task.finished_at - task.started_at)
