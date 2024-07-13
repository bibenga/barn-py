import asyncio
import logging
from contextlib import suppress
from datetime import datetime, timedelta
from random import random
from typing import Type

from asgiref.sync import sync_to_async
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from .conf import Conf
from .models import AbstractSchedule, Schedule
from .signals import post_schedule_execute, pre_schedule_execute

log = logging.getLogger(__name__)


def croniter(expr, dt):
    try:
        from croniter import croniter
    except ImportError:
        raise ValueError("croniter is not installed")
    else:
        return croniter(expr, dt)


class Scheduler:
    def __init__(
        self,
        model: Type[AbstractSchedule] | None = None,
    ) -> None:
        self._model = model or Schedule
        self._interval: float = Conf.SCHEDULE_POLL_INTERVAL.total_seconds()
        self._ttl: timedelta | None = Conf.SCHEDULE_FINISHED_TTL
        self._stop_event = asyncio.Event()
        self._thread: asyncio.Task | None = None

    async def start(self) -> None:
        self._stop_event.clear()
        self._thread = asyncio.create_task(self._run(), name="scheduler")

    async def stop(self) -> None:
        if self._thread and not self._stop_event.is_set():
            self._stop_event.set()
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
        finally:
            log.info("finished")

    async def _sleep(self) -> None:
        jitter = self._interval / 10
        timeout = self._interval + (jitter * random() - jitter / 2)
        log.debug("sleep for %.2fs", timeout)
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), 5)

    @sync_to_async
    @transaction.atomic
    def _process(self) -> None:
        schedule_qs = self._model.objects.filter(
            Q(next_run_at__isnull=True) | Q(next_run_at__lt=timezone.now()),
            is_active=True,
        ).order_by("next_run_at", "id")
        processed = 0
        for schedule in schedule_qs.select_for_update(skip_locked=True):
            self._process_one(schedule)
            processed += 1
        if processed == 0:
            log.debug("no pending schedules")

    def _process_one(self, schedule: AbstractSchedule) -> None:
        log.info("found a schedule %s", schedule.pk)

        pre_schedule_execute.send(sender=self, schedule=schedule)
        schedule.process()

        now = timezone.now()
        schedule.last_run_at = now
        if schedule.interval:
            schedule.next_run_at = now + schedule.interval
            log.info("the schedule %s is scheduled to %s",
                     schedule.pk, schedule.next_run_at)
        elif schedule.cron:
            try:
                iter = croniter(schedule.cron, schedule.next_run_at or now)
            except (TypeError, ValueError):
                log.error("the scheduler %s has an invalid cron",
                          schedule.pk, exc_info=True)
                schedule.is_active = False
            else:
                schedule.next_run_at = iter.get_next(datetime)
                log.info("the schedule %s is scheduled to %s",
                         schedule.pk, schedule.next_run_at)
        else:
            schedule.is_active = False

        post_schedule_execute.send(sender=self, schedule=schedule)
        schedule.save()

    @sync_to_async
    @transaction.atomic
    def _delete_old(self) -> None:
        moment = timezone.now() - self._ttl
        schedule_qs = self._model.objects.filter(
            is_active=False,
            next_run_at__lt=moment
        )
        deleted, _ = schedule_qs.delete()
        log.log(
            logging.DEBUG if deleted == 0 else logging.INFO,
            "deleted %d inactive schedules older than %s",
            deleted, moment
        )
