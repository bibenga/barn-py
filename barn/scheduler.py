import logging
import threading
from datetime import datetime, timedelta
from random import random
from typing import Type

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from .conf import Conf
from .models import AbstractSchedule, Schedule
from .signals import post_schedule_execute, pre_schedule_execute, remote_post_save

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

        self._stop_event = threading.Event()
        self._wakeup_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def name(self) -> str:
        return "scheduler"

    def start(self) -> None:
        self._stop_event.clear()
        self._wakeup_event.clear()
        self._thread = threading.Thread(target=self.run, name="scheduler")
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
        return bool(self._thread and self._thread.is_alive())

    def _on_remote_post_save(self, sender, **kwargs):
        log.debug("somwhere something was saved: %s", kwargs)
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

    @transaction.atomic
    def _process(self) -> None:
        schedule_qs = self._model.objects.filter(
            Q(next_run_at__isnull=True) | Q(next_run_at__lt=timezone.now()),
            is_active=True,
        ).order_by("next_run_at", "id")
        cnt = 0
        for schedule in schedule_qs.select_for_update(skip_locked=True):
            while True:
                cnt += 1
                self._process_one(schedule)
                if schedule.is_active and schedule.next_run_at and schedule.next_run_at < timezone.now():
                    continue
                break
        if cnt == 0:
            log.debug("no pending schedules")
        else:
            log.info("processed %d schedules", cnt)

    def _process_one(self, schedule: AbstractSchedule) -> None:
        log.info("found a schedule %s", schedule.pk)

        pre_schedule_execute.send(sender=self, schedule=schedule)
        schedule.process()

        now = timezone.now()
        schedule.last_run_at = now
        if schedule.interval:
            schedule.next_run_at = now + schedule.interval
            log.info("the schedule %s is scheduled to %s", schedule.pk, schedule.next_run_at)
        elif schedule.cron:
            try:
                iter = croniter(schedule.cron, schedule.next_run_at or now)
            except (TypeError, ValueError):
                log.error("the scheduler %s has an invalid cron", schedule.pk, exc_info=True)
                schedule.is_active = False
            else:
                schedule.next_run_at = iter.get_next(datetime)
                log.info("the schedule %s is scheduled to %s", schedule.pk, schedule.next_run_at)
        else:
            schedule.is_active = False

        post_schedule_execute.send(sender=self, schedule=schedule)
        schedule.save()

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
