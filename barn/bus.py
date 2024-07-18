import json
import logging
import threading
from typing import Type

from django.contrib.contenttypes.models import ContentType
from django.db import connection
from django.db.models.signals import post_save
from django.utils import timezone

from .conf import Conf
from .models import AbstractSchedule, AbstractTask, TaskStatus
from .signals import remote_post_save

log = logging.getLogger(__name__)


class PgBus:
    def __init__(
        self,
    ) -> None:
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def name(self) -> str:
        return "pg_bus"

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="pg_bus")
        self._thread.start()

    def stop(self) -> None:
        if self._thread and not self._stop_event.is_set():
            self._stop_event.set()
            self._thread.join(10)

    def is_alive(self) -> bool:
        return self._thread and self._thread.is_alive()

    def run(self) -> None:
        self._run()

    def _run(self) -> None:
        log.info("listener stated")
        try:
            with connection.cursor() as cursor:
                # a prepared statement is not supported for LISTEN operation
                cursor.execute(f"LISTEN {Conf.BUS_CHANNEL};")
                con = cursor.connection
                while not self._stop_event.is_set():
                    gen = con.notifies(timeout=5)
                    any_event = False
                    for event in gen:
                        self._send(event)
                        any_event = True
                    if not any_event:
                        log.debug("timeout...")
        finally:
            log.info("listener finished")

    def _send(self, event) -> None:
        # event: psycopg.Notify
        log.debug("event: %s", event)
        try:
            payload = json.loads(event.payload)
        except (ValueError, TypeError):
            log.warning("invalid notification payload: %s", event.payload)
        else:
            model_key = payload["model"]
            app_label, model = model_key.split(".")
            model = ContentType.objects.get_by_natural_key(app_label, model).model_class()
            instance_pk = payload["pk"]
            remote_post_save.send(sender=self, model=model, pk=instance_pk, event=payload["event"])

    @classmethod
    def connect(
        cls,
        task_model: Type[AbstractTask] | None,
        schedule_model: Type[AbstractSchedule] | None,
    ) -> None:
        if Conf.BUS:
            if task_model:
                post_save.connect(cls._on_task_post_save, sender=task_model)
            if schedule_model:
                post_save.connect(cls._on_schedule_post_save, sender=schedule_model)

    @classmethod
    def _on_task_post_save(cls, sender, instance: AbstractTask, created: bool, **kwargs) -> None:
        log.debug("the task %r is created or updated: %s", instance, created)
        if instance.status != TaskStatus.QUEUED:
            log.debug("the task %s is not in %s status", instance.pk, TaskStatus.QUEUED)
            return
        if instance.run_at > timezone.now():
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
        data = {
            "version": "1.0.0",
            "model": f"{instance._meta.app_label}.{instance._meta.model_name}",
            "pk": instance.pk,
            "event": "create" if created else "update",
        }
        payload = json.dumps(data, ensure_ascii=False)
        log.info("a message is sent in the %s channel: %s", Conf.BUS_CHANNEL, payload)
        with connection.cursor() as cursor:
            cursor.execute("select pg_notify(%s, %s)", [Conf.BUS_CHANNEL, payload])
