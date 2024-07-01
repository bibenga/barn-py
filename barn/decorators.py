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

    run_at = None
    if countdown:
        if isinstance(countdown, timedelta):
            run_at = timezone.now() + countdown
        else:
            run_at = timezone.now() + timedelta(seconds=countdown)
    elif eta:
        run_at = eta

    if run_at:
        if getattr(settings, "BARN_TASK_EAGER", False):
            raise ValueError("A task cannot be executed in eager mode")
        schedule = Task.objects.create(func=func, args=kwargs, run_at=run_at)
        log.info("the task %s is queued", task.pk)
        return schedule
    else:
        task = Task.objects.create(func=func, args=kwargs)
        log.info("the task %s is queued", task.pk)
        if getattr(settings, "BARN_TASK_EAGER", False):
            transaction.on_commit(eager_run, task)
        return task


def eager_run(task: Task) -> None:
    log.info("run the task %s in eager mode", task)
    from .worker import Worker
    worker = Worker(Task, with_deletion=False)
    worker.call_task(task)
