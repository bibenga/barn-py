# Generated by Django 5.0.7 on 2024-07-19 21:28

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("barn", "0002_schedule_interval"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="task",
            index=models.Index(
                condition=models.Q(("status", "Q")),
                fields=["run_at"],
                name="barn_task_find_next_idx",
            ),
        ),
    ]
