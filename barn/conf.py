from django.conf import settings
from django.utils.functional import classproperty


class Conf:
    # SCHEDULER_CRON = getattr(settings, "BARN_SCHEDULER_CRON", 5)  # seconds
    # TASK_TTL = getattr(settings, "BARN_TASK_TTL", 30)  # days

    @classproperty
    def USE_JITTER(cls) -> bool:
        return getattr(settings, "BARN_WITH_JITTER", False)

    @classproperty
    def TASK_EAGER(cls) -> bool:
        return getattr(settings, "BARN_TASK_EAGER", False)

    @classproperty
    def SCHEDULER_CRON(cls) -> str:
        return getattr(settings, "BARN_SCHEDULER_CRON", "* * * * *")

    @classproperty
    def SCHEDULER_DELETE_OLD(cls) -> bool:
        return getattr(settings, "BARN_SCHEDULER_DELETE_OLD", False)

    @classproperty
    def WORKER_CRON(cls) -> str:
        return getattr(settings, "BARN_WORKER_CRON", "* * * * *")

    @classproperty
    def WORKER_DELETE_OLD(cls) -> bool:
        return getattr(settings, "BARN_WORKER_DELETE_OLD", True)
