import logging
from datetime import datetime, timedelta
from functools import wraps

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from .models import Schedule, Task

log = logging.getLogger(__name__)


def task(func):
    @wraps(func)
    def delay(**kwargs) -> None:
        return async_task(func, kwargs=kwargs)

    @wraps(func)
    def apply_async(
        kwargs: dict | None = None,
        countdown: timedelta | int | float | None = None,
        eta: datetime | None = None,
    ) -> None:
        return async_task(
            func,
            kwargs=kwargs,
            countdown=countdown,
            eta=eta,
        )

    func.delay = delay
    func.apply_async = apply_async
    return func


def async_task(
    func,
    kwargs: dict | None = None,
    countdown: timedelta | int | float | None = None,
    eta: datetime | None = None,
) -> Task | Schedule:
    func = f"{func.__module__}.{func.__name__}"
    log.info("async_task: %s", func)

    next_run_at = None
    if countdown:
        if isinstance(countdown, timedelta):
            next_run_at = timezone.now() + countdown
        else:
            next_run_at = timezone.now() + timedelta(seconds=countdown)
    elif eta:
        next_run_at = eta

    if next_run_at:
        schedule = Schedule.objects.create(
            name=func,
            next_run_at=next_run_at,
            func=func,
            args=kwargs,
        )
        log.info("the schedule %s is created at %s", schedule.pk, next_run_at)
        if getattr(settings, "BARN_TASK_EAGER", False):
            raise ValueError("A task cannot be executed in eager mode")
        return schedule
    else:
        task = Task.objects.create(
            func=func,
            args=kwargs,
        )
        log.info("the task %s is queued", task.pk)
        if getattr(settings, "BARN_TASK_EAGER", False):
            transaction.on_commit(eager_run, task)
        return task


def eager_run(task: Task) -> None:
    log.info("run the task %s in eager mode", task)
    from .worker import Worker
    worker = Worker()
    worker.call_task_eager(task)
