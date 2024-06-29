
import logging
from datetime import UTC, datetime

from barn.decorators import task

log = logging.getLogger(__name__)


# from stable.example.dummy import dummy
# dummy.delay()
# dummy.apply_async(countdown=10)  # secs
@task
def dummy(**kwargs) -> str:
    log.info("dummy: %s", kwargs)
    kwargs["moment"] = datetime.now(UTC).isoformat()
    return kwargs
