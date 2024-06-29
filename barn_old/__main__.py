import logging
import logging.config

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from barn_old.lock import LockManager
from barn_old.models import Base, metadata
from barn_old.scheduler import Scheduler

logging.basicConfig(
    # format="{asctime} {levelname} [{name}] {message}",
    format="{asctime} {levelname} - {message}",
    style="{",
    level=logging.INFO,
)
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logging.getLogger('barn').setLevel(logging.DEBUG)

_l = logging.getLogger(__name__)

# pip install "psycopg[pool]"
# const dsn string = "host=host.docker.internal port=5432 user=rds password=sqlsql dbname=barn TimeZone=UTC sslmode=disable"


def main() -> None:
    _l.info("barn")

    # engine = create_engine('sqlite:///db.sqlite3')
    engine = create_engine('postgresql+psycopg://rds:sqlsql@host.docker.internal/barn')
    session_ctx = sessionmaker(engine)

    with engine.begin():
        # Base.metadata.drop_all(engine)
        # Base.metadata.create_all(engine)
        metadata.drop_all(engine)
        metadata.create_all(engine)

    # from psycopg_pool import ConnectionPool
    # pool = ConnectionPool("host=host.docker.internal port=5432 user=rds password=sqlsql dbname=barn sslmode=disable")
    # with pool.connection() as conn:
    #     _l.info(">")
    #     with conn.cursor() as cur:
    #         cur.execute("SET TIME ZONE UTC;")
    #         cur.execute("select current_timestamp, 1, ''")
    #         record = cur.fetchone()
    #         _l.info("- record: %r", record)
    #         # for record in cur:
    #         #     _l.info("- record: %r", record)
    #     _l.info("<")
    # pool.close()

    # with session_ctx() as session:
    #     cron = "* * * * * */5"
    #     now = datetime.now(UTC)
    #     iter = croniter(cron, now)
    #     e = Entry(
    #         name="e1",
    #         cron=cron,
    #         next_ts=iter.get_next(datetime),
    #         last_ts=None,
    #     )
    #     session.add(e)
    #     session.commit()
    #     logging.info("e: next_ts=%s", e.next_ts.isoformat(timespec="milliseconds"))

    lm = LockManager(engine, session_ctx)
    lm.run()

    # sh = Scheduler(session_ctx)
    # sh.delete_all()
    # sh.add("olala1", cron="* * * * * */2")
    # # sh.add("olala2", cron="* * * * * */3")
    # sh.run()


if __name__ == '__main__':
    main()
