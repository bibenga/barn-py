from datetime import timedelta

import pytest
from django.utils import timezone

from barn.decorators import task
from barn.models import Task


@task
def some_task(**kwargs) -> dict:
    return kwargs


@pytest.mark.django_db(transaction=True)
class TestDecorator:
    def test_delay(self):
        params = {"a": 1}
        some_task.delay(**params)

        task = Task.objects.get()
        assert task.func == "test_decorator.some_task"
        assert task.args == params
        assert task.run_at is not None
        assert (timezone.now() - task.run_at).total_seconds() < 1

    def test_apply_async(self):
        params = {"a": 1}
        some_task.apply_async(args=params, countdown=timedelta(hours=1, seconds=2))

        task = Task.objects.get()
        assert task.func == "test_decorator.some_task"
        assert task.args == params
        assert (task.run_at - timezone.now()).total_seconds() > 3600

    def test_cancel(self):
        some_task.delay(a=1, b=3)
        some_task.delay(a=2, b=4)

        assert Task.objects.count() == 2

        assert not some_task.cancel(a=1, b=4)
        assert Task.objects.count() == 2

        assert some_task.cancel(a=2, b=4)
        assert Task.objects.count() == 1

        task = Task.objects.get()
        assert task.args == dict(a=1, b=3)
