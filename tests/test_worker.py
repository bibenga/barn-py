import pytest

from barn.worker import Worker
from barn.models import Task


@pytest.mark.django_db(transaction=True)
class TestWorker:
    def test__call_task(self, mocker):
        task_process = mocker.patch.object(Task, "process")

        task = Task.objects.create(func="func")

        worker = Worker(Task)
        worker._call_task(task)

        task.refresh_from_db()
        assert task.is_processed
        assert task.is_success

        task_process.assert_called_once()
