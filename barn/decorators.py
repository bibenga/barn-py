import logging
from datetime import datetime, timedelta
from functools import wraps

from django.db import transaction
from django.db.models import JSONField, Q, Value
from django.utils import timezone

from .conf import Conf
from .models import Task

log = logging.getLogger(__name__)


def task(func):
    @wraps(func)
    def _delay(**kwargs) -> Task:
        return apply_async(func, args=kwargs)

    @wraps(func)
    def _apply_async(
        args: dict | None = None,
        countdown: timedelta | int | float | None = None,
        eta: datetime | None = None,
    ) -> Task:
        return apply_async(
            func,
            args=args,
            countdown=countdown,
            eta=eta,
        )

    @wraps(func)
    def _cancel(**kwargs) -> bool:
        return cancel_async(func, args=kwargs)

    func.delay = _delay
    func.apply_async = _apply_async
    func.cancel = _cancel
    return func


def apply_async(
    func,
    args: dict | None = None,
    countdown: timedelta | int | float | None = None,
    eta: datetime | None = None,
) -> Task:
    run_at = None
    if countdown:
        if isinstance(countdown, timedelta):
            run_at = timezone.now() + countdown
        else:
            run_at = timezone.now() + timedelta(seconds=countdown)
    elif eta:
        run_at = eta

    task = Task.objects.create(
        func=f"{func.__module__}.{func.__name__}",
        args=args,
        run_at=run_at,
    )
    log.info("the task %s is queued", task.pk)

    if Conf.TASK_SYNC:
        if run_at:
            raise RuntimeError("A task cannot be executed in eager mode")

        def _call() -> None:
            log.warning("run the task %s in sync mode", task)
            from .worker import Worker
            worker = Worker(Task)
            worker.sync_call_task(task)

        transaction.on_commit(_call)

    return task


def cancel_async(
    func,
    args: dict | None = None,
) -> bool:
    q = Q(func=f"{func.__module__}.{func.__name__}")
    if args is None:
        q &= Q(args=None) | Q(args=Value(None, JSONField()))
    elif args:
        # q = Q(args__contains=kwargs)
        for key, value in args.items():
            q &= Q(**{f"args__{key}": value})
    deleted, _ = Task.objects.filter(q).delete()
    return deleted > 0
