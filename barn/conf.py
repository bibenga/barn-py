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
    def SCHEDULE_DELETE_OLD(cls) -> bool:
        return getattr(settings, "BARN_SCHEDULE_DELETE_OLD", False)

    def SCHEDULE_DELETE_OLDER_THAN(cls) -> timedelta:
        return timedelta(days=getattr(settings, "BARN_SCHEDULE_DELETE_OLDER_THAN", 30))

    @classproperty
    def TASK_SYNC(cls) -> bool:
        return getattr(settings, "BARN_TASK_SYNC", False)

    @classproperty
    def TASK_POLL_CRON(cls) -> str:
        return getattr(settings, "BARN_TASK_POLL_CRON", "* * * * *")

    @classproperty
    def TASK_DELETE_OLD(cls) -> bool:
        return getattr(settings, "BARN_TASK_DELETE_OLD", True)

    @classproperty
    def TASK_DELETE_OLDER_THAN(cls) -> timedelta:
        return timedelta(days=getattr(settings, "BARN_TASK_DELETE_OLDER_THAN", 30))
