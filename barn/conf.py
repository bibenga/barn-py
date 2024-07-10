from datetime import timedelta
from django.conf import settings
from django.utils.functional import classproperty


class MetaConf(type):
    # def USE_JITTER(cls) -> bool:
    #     return getattr(settings, "BARN_WITH_JITTER", False)
    pass


class Conf(object, metaclass=MetaConf):
    @classproperty
    def SCHEDULE_POLL_CRON(cls) -> str:
        return getattr(settings, "BARN_SCHEDULE_POLL_CRON", "* * * * *")

    @classproperty
    def SCHEDULE_FINISHED_TTL(cls) -> timedelta | None:
        value = getattr(settings, "BARN_SCHEDULE_FINISHED_TTL", None)
        if not value:
            return None
        return as_timedelta(value, timedelta(days=30))

    @classproperty
    def TASK_SYNC(cls) -> bool:
        return getattr(settings, "BARN_TASK_SYNC", False)

    @classproperty
    def TASK_POLL_CRON(cls) -> str:
        return getattr(settings, "BARN_TASK_POLL_CRON", "* * * * *")

    @classproperty
    def TASL_POLL_INTERVAL(cls) -> timedelta:
        return as_timedelta(getattr(settings, "BARN_TASL_POLL_INTERVAL", None),
                            timedelta(seconds=30))

    @classproperty
    def TASK_FINISHED_TTL(cls) -> timedelta | None:
        value = getattr(settings, "BARN_TASK_FINISHED_TTL", None)
        if not value:
            return None
        return as_timedelta(value, timedelta(days=30))


def as_timedelta(value: None | int | float | timedelta, deault: timedelta) -> timedelta:
    if not value:
        return deault
    elif isinstance(value, timedelta):
        return value
    elif isinstance(value, (int, float)):
        return timedelta(seconds=value)
    raise ValueError("incorrect value")
