# Generated by Django 4.1.3 on 2022-11-01 21:43

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("robust", "0005_alter_task_payload"),
    ]

    operations = [
        migrations.AlterField(
            model_name="task",
            name="id",
            field=models.BigAutoField(
                auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
            ),
        ),
        migrations.AlterField(
            model_name="taskevent",
            name="id",
            field=models.BigAutoField(
                auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
            ),
        ),
    ]