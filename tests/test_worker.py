import pytest

from barn.models import Task, TaskStatus
from barn.worker import Worker


@pytest.mark.django_db(transaction=True)
class TestWorker:
    async def test__process(self, mocker):
        _process_next = mocker.patch.object(Worker, "_process_next", side_effect=[True, False])

        worker = Worker()
        await worker._process()

        assert _process_next.call_count == 2

    async def test__process_next(self, mocker):
        _process_one = mocker.patch.object(Worker, "_process_one")

        worker = Worker()
        await worker._process_next()
        _process_one.assert_not_called()

        await Task.objects.acreate(func="func")
        await worker._process_next()
        _process_one.assert_called_once()

    def test__process_one(self, mocker):
        task_process = mocker.patch.object(Task, "process")

        task = Task.objects.create(func="func")

        worker = Worker()
        worker._process_one(task)

        task.refresh_from_db()
        assert task.status == TaskStatus.DONE

        task_process.assert_called_once()

    def test__process_one_with_error(self, mocker):
        code = "71ADA163-9EF9-4FB5-8563-29F32009E09B"
        task_process = mocker.patch.object(Task, "process", side_effect=RuntimeError(code))

        task = Task.objects.create(func="func")

        worker = Worker()
        worker._process_one(task)

        task.refresh_from_db()
        assert task.status == TaskStatus.FAILED
        assert code in str(task.error)

        task_process.assert_called_once()