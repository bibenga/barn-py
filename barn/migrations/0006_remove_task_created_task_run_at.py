# Generated by Django 5.0.6 on 2024-07-01 11:51

import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("barn", "0005_alter_schedule_cron_alter_schedule_func_and_more"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="task",
            name="created",
        ),
        migrations.AddField(
            model_name="task",
            name="run_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=django.utils.timezone.now
            ),
            preserve_default=False,
        ),
    ]