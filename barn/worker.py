import logging
import signal
import traceback
from datetime import UTC, datetime, timedelta
from threading import Event, Thread

from django.db import transaction
from django.db.models import Q
from django.utils.module_loading import import_string

from .models import Task

log = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        task_filter: list[str] | None = None,
        interval: float | int = 5,
    ) -> None:
        self._task_filter = task_filter
        self._interval = interval
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
            while not self._stop_event.wait(self._interval):
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
            log.info("deleted %d old tasks", deleted)

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
