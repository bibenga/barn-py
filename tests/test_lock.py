import pytest


@pytest.mark.django_db(transaction=True)
class TestLock:
    def test_loc(self):
        pass
