import pytest

from barn.elector import LeaderElector
from barn.models import Lock


@pytest.mark.django_db(transaction=True)
class TestLock:
    def test_lock(self):
        elector = LeaderElector()
        is_acquired = elector._acquire()
        assert is_acquired

        lock = Lock.objects.get()
        assert lock.locked_at == elector._locked_at
        assert lock.owner == elector._hostname

    def test_confirm(self):
        elector = LeaderElector()

        is_confirmed = elector._confirm()
        assert not is_confirmed

        is_acquired = elector._acquire()
        assert is_acquired
        lock = Lock.objects.get()
        assert lock.locked_at == elector._locked_at
        assert lock.owner == elector._hostname

        is_confirmed = elector._confirm()
        assert is_confirmed
        lock = Lock.objects.get()
        assert lock.locked_at == elector._locked_at
        assert lock.owner == elector._hostname

    def test_release(self):
        elector = LeaderElector()

        is_acquired = elector._acquire()
        assert is_acquired
        lock = Lock.objects.get()
        assert lock.locked_at == elector._locked_at
        assert lock.owner == elector._hostname

        elector._release()
        lock = Lock.objects.get()
        assert lock.locked_at is None
        assert lock.owner is None
