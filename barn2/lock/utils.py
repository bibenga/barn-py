
import logging
from datetime import UTC, datetime
from .decorators import task

log = logging.getLogger(__name__)


def set_proc_title(name: str) -> None:
    try:
        from setproctitle import setproctitle
    except ImportError:
        pass
    else:
        setproctitle(name)


# from barn2.lock.utils import dummy
# dummy.delay()
# dummy.apply_async(countdown=10)  # secs
@task
def dummy(**kwargs) -> str:
    # barn2.lock.utils.dummy
    log.info("dummy: %s", kwargs)
    kwargs["moment"] = datetime.now(UTC).isoformat()
    return kwargs
