import pytest
from django.utils import timezone

from barn.models import Schedule
from barn.scheduler import Scheduler


@pytest.mark.django_db(transaction=True)
class TestScheduler:
    async def test__process(self, mocker):
        _process_one = mocker.patch.object(Scheduler, "_process_one")

        scheduler = Scheduler()
        await scheduler._process()
        _process_one.assert_not_called()

        await Schedule.objects.acreate(cron="* * * * *", is_active=False)
        await scheduler._process()
        _process_one.assert_not_called()

        await Schedule.objects.acreate(cron="* * * * *")
        await scheduler._process()
        _process_one.assert_called_once()

    def test__process_one_oneshot(self, mocker):
        schedule_process = mocker.patch.object(Schedule, "process")

        schedule = Schedule.objects.create(next_run_at=timezone.now())

        scheduler = Scheduler()
        scheduler._process_one(schedule)

        schedule.refresh_from_db()
        assert not schedule.is_active

        schedule_process.assert_called_once()

    def test__process_one_cron(self, mocker):
        schedule_process = mocker.patch.object(Schedule, "process")

        schedule = Schedule.objects.create(cron="* * * * * ")

        scheduler = Scheduler()
        scheduler._process_one(schedule)

        schedule_process.assert_not_called()
        schedule.refresh_from_db()
        assert schedule.is_active
        assert schedule.next_run_at is not None

        scheduler._process_one(schedule)
        schedule_process.assert_called_once()
