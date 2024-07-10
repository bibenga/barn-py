import logging
from datetime import datetime, timedelta
from functools import partial, wraps

from django.db import transaction
from django.utils import timezone

from .conf import Conf
from .models import Schedule, Task

log = logging.getLogger(__name__)


def task(func):
    @wraps(func)
    def delay(**kwargs) -> Task:
        return async_task(func, kwargs=kwargs)

    @wraps(func)
    def apply_async(
        kwargs: dict | None = None,
        countdown: timedelta | int | float | None = None,
        eta: datetime | None = None,
    ) -> Task:
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
) -> Task:
    func = f"{func.__module__}.{func.__name__}"
    run_at = None
    if countdown:
        if isinstance(countdown, timedelta):
            run_at = timezone.now() + countdown
        else:
            run_at = timezone.now() + timedelta(seconds=countdown)
    elif eta:
        run_at = eta

    task = Task.objects.create(func=func, args=kwargs, run_at=run_at)
    log.info("the task %s is queued", task.pk)

    if Conf.TASK_SYNC:
        if run_at:
            raise RuntimeError("A task cannot be executed in eager mode")
        transaction.on_commit(partial(_sync_call, task))

    return task


def _sync_call(task: Task) -> None:
    log.info("run the task %s in sync mode", task)
    from .worker import Worker
    worker = Worker(Task)
    worker.sync_call_task(task)
