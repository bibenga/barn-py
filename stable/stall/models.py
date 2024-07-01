from django.db import models

from barn.models import AbstractSchedule, AbstractTask


class Schedule1(AbstractSchedule):
    arg1 = models.IntegerField()
    arg2 = models.CharField(max_length=10)

    def process(self) -> None:
        self.arg1 += 1
        Task1.objects.create(arg1=self.arg1, arg2=self.arg2)


class Task1(AbstractTask):
    arg1 = models.IntegerField()
    arg2 = models.CharField(max_length=10)

    def process(self) -> None:
        self.arg1 += 1
        self.arg2 = f"{self.arg2}:{self.arg1}"


class Schedule2(AbstractSchedule):
    arg1 = models.IntegerField()

    def process(self) -> None:
        self.arg1 -= 1
        Task2.objects.create(arg1=self.arg1)


class Task2(AbstractTask):
    arg1 = models.IntegerField()

    def process(self) -> None:
        self.arg1 += 1
