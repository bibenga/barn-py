import logging
from random import random
import signal
import traceback
from datetime import UTC, datetime, timedelta
from threading import Event, Thread

from croniter import croniter
from django.db import transaction
from django.db.models import Q
from django.utils.module_loading import import_string

from barn.conf import Conf

from .models import Task

log = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        task_filter: list[str] | None = None,
    ) -> None:
        self._cron = "* * * * * */5"
        self._task_filter = task_filter
        self._thread: Thread | None = None
        self._stop_event = Event()

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = Thread(name="broker", target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if not self._stop_event.is_set():
            self._stop_event.set()
            self._thread.join(5)

    def _run(self) -> None:
        log.info("stated")
        try:
            self._process()
            self._delete_old()
            while not self._stop_event.is_set():
                now = datetime.now(UTC)
                iter = croniter(self._cron, now)
                next_run_at = iter.get_next(datetime)
                sleep_seconds = next_run_at - now
                # add some jitter
                # if Conf.USE_JITTER:
                #     sleep_seconds += timedelta(seconds=random() / 5)
                log.info("sleep for %s", sleep_seconds)
                if self._stop_event.wait(sleep_seconds.total_seconds()):
                    break
                self._process()
                self._delete_old()
        finally:
            log.info("finished")

    def _sig_handler(self, signum, frame) -> None:
        log.info("got signal - %s", signal.strsignal(signum))
        self._stop_event.set()

    def _process(self) -> None:
        while not self._stop_event.is_set():
            with transaction.atomic():
                task_qs = Task.objects.filter(is_processed=False).order_by("created", "id")
                if self._task_filter:
                    f = Q()
                    for name in self._task_filter:
                        f |= Q(func__startswith=name)
                    task_qs = task_qs.filter(f)
                task = task_qs.select_for_update(skip_locked=True).first()
                if not task:
                    log.info("no pending task is found")
                    break
                log.info("found a task %s", task.pk)

                self._call_task(task)

    def _delete_old(self) -> None:
        with transaction.atomic():
            moment = datetime.now(UTC) - timedelta(days=3)
            task_qs = Task.objects.filter(
                is_processed=True,
                created__lt=moment
            )
            deleted, _ = task_qs.delete()
            log.log(
                logging.DEBUG if deleted == 0 else logging.INFO,
                "deleted %d old tasks older than %s",
                deleted, moment
            )

    def call_task_eager(self, task: Task) -> None:
        self._call_task(task)

    def _call_task(self, task: Task) -> None:
        log.info("process the task %s task", task)
        task.started_at = datetime.now(UTC)
        try:
            # pre_execute.send(sender=self, task=task)
            func = import_string(task.func)
            with transaction.atomic():
                task.result = func(**(task.args or {}))
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
            task.finished_at = datetime.now(UTC)
            task.save(update_fields=["is_processed", "started_at",
                                     "finished_at", "is_success", "result", "error"])

            log.info("the task %s is processed in %s", task.pk,
                     task.finished_at - task.started_at)
