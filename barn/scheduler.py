import logging
from datetime import datetime
from threading import Event, Thread
from typing import Type

from croniter import croniter
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from .conf import Conf
from .models import AbstractSchedule, Schedule
from .signals import schedule_execute

log = logging.getLogger(__name__)


class Scheduler:
    def __init__(
        self,
        model: Type[AbstractSchedule] | None,
    ) -> None:
        self._model = model or Schedule
        self._cron = Conf.SCHEDULE_POLL_CRON
        self._ttl = Conf.SCHEDULE_FINISHED_TTL
        self._thread: Thread | None = None
        self._stop_event = Event()

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = Thread(name="scheduler", target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if not self._stop_event.is_set():
            self._stop_event.set()
            self._thread.join(60)

    def _run(self) -> None:
        log.info("stated")
        try:
            self._process()
            self._delete_old()

            while not self._stop_event.is_set():
                now = timezone.now()
                iter = croniter(self._cron, now)
                next_run_at = iter.get_next(datetime)
                sleep_seconds = next_run_at - now
                log.info("sleep for %s", sleep_seconds)
                if self._stop_event.wait(sleep_seconds.total_seconds()):
                    break

                self._process()
                self._delete_old()
        finally:
            log.info("finished")

    def _process(self) -> None:
        while not self._stop_event.is_set():
            with transaction.atomic():
                schedule_qs = self._model.objects.filter(
                    Q(next_run_at__isnull=True) | Q(next_run_at__lt=timezone.now()),
                    is_active=True,
                ).order_by("next_run_at", "id")
                schedule = schedule_qs.select_for_update(skip_locked=True).first()
                if schedule:
                    self._process_schedule(schedule)
                else:
                    log.info("no pending schedule is found")
                    break

    def _process_schedule(self, schedule: AbstractSchedule) -> None:
        log.info("found a schedule %s", schedule.pk)

        if not schedule.next_run_at and not schedule.cron:
            log.info("the schedule %s is an invalid", schedule.pk)
            schedule.is_active = False
            schedule.save(update_fields=["is_active"])
            return

        now = timezone.now()
        if not schedule.next_run_at:
            try:
                iter = croniter(schedule.cron, now)
            except (TypeError, ValueError):
                log.error("the schedule %s has an invalid cron", schedule.pk, exc_info=True)
                schedule.is_active = False
                schedule.save(update_fields=["is_active"])
            else:
                schedule.next_run_at = iter.get_next(datetime)
                log.info("the schedule %s is scheduled to %s", schedule.pk, schedule.next_run_at)
                schedule.save(update_fields=["next_run_at"])
            # it will be called on next iteration if the next_run_at is near to now
            return

        schedule_execute.send(sender=self, schedule=schedule)
        schedule.process()

        now = timezone.now()
        schedule.last_run_at = now
        if schedule.cron:
            try:
                iter = croniter(schedule.cron, schedule.next_run_at or now)
            except (TypeError, ValueError):
                log.error("the scheduler %s has an invalid cron",
                          schedule.pk, exc_info=True)
                schedule.is_active = False
                # schedule.save(update_fields=["is_active", "last_run_at"])
            else:
                schedule.next_run_at = iter.get_next(datetime)
                log.info("the schedule %s is scheduled to %s",
                         schedule.pk, schedule.next_run_at)
                # schedule.save(update_fields=["next_run_at", "last_run_at"])
        else:
            schedule.is_active = False
            # schedule.save(update_fields=["is_active", "last_run_at"])
        schedule.save()

    def _delete_old(self) -> None:
        if self._ttl is None:
            return
        with transaction.atomic():
            moment = timezone.now() - self._ttl
            schedule_qs = self._model.objects.filter(
                is_active=False,
                next_run_at__lt=moment
            )
            deleted, _ = schedule_qs.delete()
            log.log(
                logging.DEBUG if deleted == 0 else logging.INFO,
                "deleted %d schedules older than %s",
                deleted, moment
            )