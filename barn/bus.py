import json
import logging
import threading
from datetime import timedelta
from typing import Type

from django.apps import apps
from django.db import connection
from django.db.models.signals import post_save
from django.utils import timezone

from .conf import Conf
from .models import AbstractSchedule, AbstractTask, TaskStatus
from .signals import remote_post_save

log = logging.getLogger(__name__)


class PgBus:
    def __init__(self, *listen_models: Type[AbstractTask | AbstractSchedule]) -> None:
        self._models = listen_models
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def name(self) -> str:
        return "pg_bus"

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(target=self.run, name="pg_bus")
        self._thread.start()

    def stop(self) -> None:
        if self._thread and not self._stop_event.is_set():
            self._stop_event.set()
            self._thread.join(10)

    def is_alive(self) -> bool:
        return self._thread and self._thread.is_alive()

    def run(self) -> None:
        log.info("stated")
        try:
            self._run()
        except:
            log.fatal("failed")
            raise
        finally:
            log.info("finished")

    def _run(self) -> None:
        if not self._models:
            raise ValueError("the models is not provided")

        channels = {
            Conf.BUS_CHANNEL % {
                "app_label": model._meta.app_label,
                "model_name": model._meta.model_name,
            }
            for model in self._models
        }

        with connection.cursor() as cursor:
            # a prepared statement is not supported for LISTEN operation
            for channel in channels:
                log.info("listen on %r", channel)
                cursor.execute(f"LISTEN {channel};")
            try:
                con = cursor.connection
                while not self._stop_event.is_set():
                    cnt = 0
                    gen = con.notifies(timeout=5)
                    for event in gen:
                        self._send(event)
                        cnt += 1
                    if cnt > 0:
                        log.info("processed %d events", cnt)
                    else:
                        log.debug("i am alive...")
            finally:
                for channel in channels:
                    log.info("unlisten from %s", channel)
                    cursor.execute(f"UNLISTEN {channel};")

    def _send(self, event) -> None:
        # event: psycopg.Notify
        log.debug("event: %s", event)
        try:
            payload = json.loads(event.payload)
        except (ValueError, TypeError):
            log.warning("invalid notification payload: %s", event.payload)
        else:
            model_key = payload["model"]
            app_label, model_name = model_key.split(".")
            model = apps.get_model(app_label, model_name)
            instance_pk = payload["pk"]
            remote_post_save.send(sender=self, model=model, pk=instance_pk, event=payload["event"])

    @classmethod
    def connect(cls, *models: Type[AbstractTask | AbstractSchedule]) -> None:
        if not Conf.BUS_ENABLED:
            return
        for model in models:
            log.info("connect post_save listener to %s", model)
            if issubclass(model, AbstractTask):
                post_save.connect(cls._on_task_post_save, sender=model)
            elif issubclass(model, AbstractSchedule):
                post_save.connect(cls._on_schedule_post_save, sender=model)
            else:
                raise ValueError(f"the model '{model}' is invalid")

    @classmethod
    def disconnect(cls, *models: Type[AbstractTask | AbstractSchedule]) -> None:
        if not Conf.BUS_ENABLED:
            return
        for model in models:
            log.info("disconnect post_save listener from %s", model)
            if issubclass(model, AbstractTask):
                post_save.disconnect(cls._on_task_post_save, sender=model)
            elif issubclass(model, AbstractSchedule):
                post_save.disconnect(cls._on_schedule_post_save, sender=model)
            else:
                raise ValueError(f"the model '{model}' is invalid")

    @classmethod
    def _on_task_post_save(cls, sender, instance: AbstractTask, created: bool, **kwargs) -> None:
        log.debug("the task %r is created or updated: %s", instance, created)
        if instance.status != TaskStatus.QUEUED:
            log.debug("the task %s is not in %s status", instance.pk, TaskStatus.QUEUED)
            return
        if instance.run_at > (timezone.now() + timedelta(microseconds=1)):
            log.debug("the task %s is in the future: %s", instance.pk, instance.run_at)
            return
        cls._enqueue_remote_post_save(instance, created)

    @classmethod
    def _on_schedule_post_save(cls, sender, instance: AbstractSchedule, created: bool, **kwargs) -> None:
        log.debug("the schedule %r is created or updated: %s", instance.pk, created)
        if not instance.is_active:
            log.debug("the schedule %s is not active", instance)
            return
        if instance.next_run_at and instance.next_run_at > timezone.now():
            log.debug("the schedule %r is in the future: %s", instance.pk, instance.next_run_at)
            return
        cls._enqueue_remote_post_save(instance, created)

    @classmethod
    def _enqueue_remote_post_save(cls, instance: AbstractTask | AbstractSchedule, created: bool) -> None:
        app_label, model_name = instance._meta.app_label, instance._meta.model_name
        data = {
            "version": "1.0.0",
            "model": f"{app_label}.{model_name}",
            "pk": instance.pk,
            "event": "create" if created else "update",
        }
        payload = json.dumps(data, ensure_ascii=False)
        channel = Conf.BUS_CHANNEL % {"app_label": app_label, "model_name": model_name}
        log.info("a message is sent in the %s channel: %s", channel, payload)
        with connection.cursor() as cursor:
            cursor.execute("select pg_notify(%s, %s)", [channel, payload])
