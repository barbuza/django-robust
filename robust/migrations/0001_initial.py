from typing import List, Tuple

import django.contrib.postgres.fields
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies: List[Tuple[str, str]] = []

    operations = [
        migrations.CreateModel(
            name="Task",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "status",
                    models.PositiveSmallIntegerField(
                        choices=[
                            (0, "pending"),
                            (1, "retry"),
                            (2, "succeed"),
                            (3, "failed"),
                        ],
                        db_index=True,
                        default=0,
                    ),
                ),
                ("name", models.TextField()),
                (
                    "payload",
                    django.contrib.postgres.fields.jsonb.JSONField(
                        blank=True, null=True
                    ),
                ),
                ("eta", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
        ),
        migrations.CreateModel(
            name="TaskEvent",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("created_at", models.DateTimeField()),
                (
                    "status",
                    models.PositiveSmallIntegerField(
                        choices=[
                            (0, "pending"),
                            (1, "retry"),
                            (2, "succeed"),
                            (3, "failed"),
                        ]
                    ),
                ),
                (
                    "task",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="events",
                        to="robust.Task",
                    ),
                ),
                ("eta", models.DateTimeField(blank=True, null=True)),
            ],
        ),
        migrations.AddField(
            model_name="task",
            name="tags",
            field=django.contrib.postgres.fields.ArrayField(
                base_field=models.TextField(), default=[], size=None
            ),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name="task",
            name="payload",
            field=django.contrib.postgres.fields.jsonb.JSONField(default={}),
            preserve_default=False,
        ),
        migrations.CreateModel(
            name="RateLimitRun",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("created_at", models.DateTimeField(db_index=True)),
                ("tag", models.TextField()),
            ],
        ),
        migrations.AlterIndexTogether(
            name="task",
            index_together={("status", "eta")},
        ),
    ]
