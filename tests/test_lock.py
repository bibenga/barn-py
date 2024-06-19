from sqlalchemy.orm import Session


class TestLock:

    def test_1(self, session: Session) -> None:
        assert 1 == 1
