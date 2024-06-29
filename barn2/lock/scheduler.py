import logging
import signal
from datetime import UTC, datetime, timedelta
from threading import Event, Thread

from croniter import croniter
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from .models import Schedule, Task
from .signals import schedule_execute

log = logging.getLogger(__name__)


class SimpleScheduler:
    def __init__(
        self,
        use_signals: bool = True,
        interval: float | int = 5,
    ) -> None:
        self._use_signals = use_signals
        self._interval = interval
        self._thread: Thread | None = None
        self._stop_event = Event()
        self._stoped_event = Event()

    def run(self) -> None:
        if self._use_signals:
            signal.signal(signal.SIGTERM, self._sig_handler)
            signal.signal(signal.SIGINT, self._sig_handler)
        self._run()

    def _sig_handler(self, signum, frame) -> None:
        log.info("got signal - %s", signal.strsignal(signum))
        self._stop_event.set()

    def start(self) -> None:
        self._thread = Thread(name="scheduler", target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._stoped_event.wait(5)

    def _run(self) -> None:
        log.info("stated")
        try:
            self._process()
            while not self._stop_event.wait(self._interval):
                self._process()
        finally:
            self._stoped_event.set()
            log.info("finished")

    def _process(self) -> None:
        while not self._stop_event.is_set():
            with transaction.atomic():
                schedule_qs = Schedule.objects.filter(
                    Q(next_run_at__isnull=True) | Q(next_run_at__lt=timezone.now()),
                    is_active=True,
                ).order_by("next_run_at", "id")
                schedule = schedule_qs.select_for_update(skip_locked=True).first()
                if not schedule:
                    log.info("no pending schedule is found")
                    break

                self._process_schedule(schedule)

    def _process_schedule(self, schedule: Schedule) -> None:
        log.info("found a schedule %s", schedule.pk)

        if not schedule.next_run_at and not schedule.cron:
            log.info("the schedule %s is an invalid", schedule.pk)
            schedule.is_active = False
            schedule.save(update_fields=["is_active"])
            return

        now = datetime.now(UTC)
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
        task = Task.objects.create(func=schedule.func, args=schedule.args)
        log.info("the task %s is created for schedule %s", task.pk, schedule.pk)

        now = datetime.now(UTC)
        schedule.last_run_at = now
        if schedule.cron:
            try:
                iter = croniter(schedule.cron, schedule.next_run_at or now)
            except (TypeError, ValueError):
                log.error("the scheduler %s has an invalid cron",
                          schedule.pk, exc_info=True)
                schedule.is_active = False
                schedule.save(update_fields=["is_active", "last_run_at"])
            else:
                schedule.next_run_at = iter.get_next(datetime)
                log.info("the schedule %s is scheduled to %s",
                         schedule.pk, schedule.next_run_at)
                schedule.save(update_fields=["next_run_at", "last_run_at"])
        else:
            schedule.is_active = False
            schedule.save(update_fields=["is_active", "last_run_at"])

    @transaction.atomic
    def _delete_old(self) -> None:
        with transaction.atomic():
            moment = datetime.now(UTC) - timedelta(days=3)
            schedule_qs = Schedule.objects.filter(
                is_active=False,
                next_run_at__lt=moment
            )
            deleted, _ = schedule_qs.delete()
            log.info("deleted %d old schedule entries", deleted)


class Scheduler:
    def __init__(
        self,
        use_signals: bool = True,
        interval: float | int = 5,
    ) -> None:
        self._use_signals = use_signals
        self._interval = interval
        self._thread: Thread | None = None
        self._stop_event = Event()
        self._stoped_event = Event()
        self._reload_schedule = Schedule(pk=-1, name="<reload>", cron="* * * * * */10")
        self._schedules: dict[int, Schedule] = {}

    def run(self) -> None:
        if self._use_signals:
            signal.signal(signal.SIGTERM, self._sig_handler)
            signal.signal(signal.SIGINT, self._sig_handler)
        self._run()

    def _sig_handler(self, signum, frame) -> None:
        log.info("got signal - %s", signal.strsignal(signum))
        self._stop_event.set()

    def start(self) -> None:
        self._thread = Thread(name="scheduler", target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._stoped_event.wait(5)

    def _run(self) -> None:
        iter = croniter(self._reload_schedule.cron, datetime.now(UTC))
        self._reload_schedule.next_run_at = iter.get_next(datetime)

        self._reload()

        while True:
            schedule = self._get_next_schedule()
            if schedule.next_run_at is None:
                raise RuntimeError("code bug: next_run_at is None")

            log.info("next schedule is %s", schedule.id)

            now = datetime.now(UTC)
            if schedule.next_run_at > now:
                sleep_seconds: timedelta = schedule.next_run_at - now
                log.info("sleep for %s", sleep_seconds)
                if self._stop_event.wait(sleep_seconds.total_seconds()):
                    break

            if schedule is self._reload_schedule:
                self._reload()
            else:
                self._process_schedule(schedule)

    def _reload(self) -> None:
        log.info("reload schedules")
        db_schedules = self._get_schedules()

        newKeys = set(db_schedules.keys())
        keys = set(self._schedules.keys())

        added = newKeys - keys
        changedOrNot = newKeys & keys
        changed = set[int]()
        deleted = keys - newKeys

        for pk in added:
            self._schedules[pk] = db_schedules[pk]

        for pk in changedOrNot:
            schedule = self._schedules[pk]
            db_schedule = db_schedules[pk]
            if self._is_changed(schedule, db_schedule):
                # only fo log
                changed.add(pk)
            self._schedules[pk] = db_schedules[pk]

        for pk in deleted:
            del self._schedules[pk]

        log.info("all: active=%d, added: %r; changed: %r; deleted=%r",
                 len(self._schedules), added, changed, deleted)

        iter = croniter(self._reload_schedule.cron,
                        self._reload_schedule.next_run_at or datetime.now(UTC))
        self._reload_schedule.next_run_at = iter.get_next(datetime)

    @transaction.atomic
    def _get_schedules(self) -> dict[int, Schedule]:
        schedules = dict[int, Schedule]()
        schedule_qs = Schedule.objects.filter(is_active=True)
        for schedule in schedule_qs.select_for_update():
            log.debug("loaded schedule %s", schedule.pk)

            if not schedule.next_run_at and not schedule.cron:
                log.info("the schedule %s is an invalid", schedule.pk)
                schedule.is_active = False
                schedule.save(update_fields=["is_active"])
                continue

            if not schedule.next_run_at:
                try:
                    iter = croniter(schedule.cron, datetime.now(UTC))
                except (TypeError, ValueError):
                    log.error("the scheduler %s has an invalid cron", schedule.pk, exc_info=True)
                    schedule.is_active = False
                    schedule.save(update_fields=["is_active"])
                    continue
                schedule.next_run_at = iter.get_next(datetime)
                log.info("the schedule %s is scheduled to %s", schedule.pk, schedule.next_run_at)
                schedule.save(update_fields=["next_run_at"])

            schedules[schedule.pk] = schedule

        log.info("found %d entries", len(schedules))
        return schedules

    def _is_changed(self, s1: Schedule, s2: Schedule) -> bool:
        if s1.cron != s2.cron:
            return True
        if s1.next_run_at != s2.next_run_at:
            return True
        return False

    def _get_next_schedule(self) -> Schedule:
        # entry: Entry | None = None
        schedule = self._reload_schedule
        for s in self._schedules.values():
            if s.next_run_at < schedule.next_run_at:
                schedule = s
        return schedule

    @transaction.atomic
    def _process_schedule(self, schedule: Schedule) -> None:
        log.info("process %s schedule", schedule.pk)

        schedule_qs = Schedule.objects.filter(is_active=True, pk=schedule.pk)
        db_schedule = schedule_qs.select_for_update().first()
        if db_schedule is None:
            log.error("the schedule %s was deleted or it is inactive", schedule.pk)
            del self._schedules[schedule.pk]
            return

        self._schedules[db_schedule.pk] = db_schedule

        if not db_schedule.next_run_at and not db_schedule.cron:
            log.info("the schedule %s is an invalid", db_schedule.pk)
            db_schedule.is_active = False
            db_schedule.save(update_fields=["is_active"])
            del self._schedules[db_schedule.pk]
            return

        if not db_schedule.next_run_at:
            try:
                iter = croniter(db_schedule.cron, db_schedule.next_run_at or datetime.now(UTC))
            except (TypeError, ValueError):
                log.error("the schedule %s has an invalid cron",
                          db_schedule.pk, exc_info=True)
                db_schedule.is_active = False
                db_schedule.save(update_fields=["is_active"])
                del self._schedules[db_schedule.pk]
            else:
                db_schedule.next_run_at = iter.get_next(datetime)
                log.info("the schedule %s is scheduled for %s",
                         db_schedule.pk, db_schedule.next_run_at)
                db_schedule.save(update_fields=["next_run_at"])
            return

        if schedule.next_run_at != db_schedule.next_run_at:
            return

        # call
        schedule_execute.send(sender=self, schedule=db_schedule)
        task = Task.objects.create(func=db_schedule.func, args=db_schedule.args)
        log.info("the task %s is created for schedule %s", task.pk, db_schedule.pk)

        if db_schedule.cron:
            try:
                iter = croniter(db_schedule.cron, db_schedule.next_run_at or datetime.now(UTC))
            except (TypeError, ValueError):
                log.error("the schedule %s has an invalid cron",
                          db_schedule.pk, exc_info=True)
                db_schedule.is_active = False
                db_schedule.save(update_fields=["is_active"])
                del self._schedules[db_schedule.pk]
            else:
                db_schedule.next_run_at = iter.get_next(datetime)
                log.info("the schedule %s is scheduled for %s",
                         db_schedule.pk, db_schedule.next_run_at)
                db_schedule.save(update_fields=["next_run_at"])
        else:
            db_schedule.is_active = False
            db_schedule.last_run_at = datetime.now(UTC)
            db_schedule.save(update_fields=["is_active", "last_run_at"])
            del self._schedules[db_schedule.pk]

    @transaction.atomic
    def _delete_old(self) -> None:
        moment = datetime.now(UTC) - timedelta(days=3)
        schedule_qs = Schedule.objects.filter(
            is_active=False,
            next_run_at__lt=moment
        )
        deleted, _ = schedule_qs.delete()
        log.info("deleted %d old schedule entries", deleted)
