from typing import List, Optional, Tuple, cast

from django.contrib import admin, messages
from django.http import HttpRequest
from django.utils.safestring import mark_safe
from django_object_actions import (
    BaseDjangoObjectActions,
    takes_instance_or_queryset,
)
from pygments import highlight
from pygments.formatters.html import HtmlFormatter
from pygments.lexers.python import Python3TracebackLexer

from .models import Task, TaskQuerySet, unwrap_payload


class TaskEventsFilter(admin.SimpleListFilter):
    SUCCEED = "succeed"
    TROUBLED = "troubled"

    title = parameter_name = "events"

    def lookups(
        self, request: HttpRequest, model_admin: "TaskAdmin"
    ) -> List[Tuple[str, str]]:
        return [
            (self.SUCCEED, "Succeed"),
            (self.TROUBLED, "Troubled"),
        ]

    def queryset(self, request: HttpRequest, queryset: TaskQuerySet) -> TaskQuerySet:
        if self.value() == self.TROUBLED:
            queryset = queryset.with_fails()
        elif self.value() == self.SUCCEED:
            queryset = queryset.without_fails()
        return queryset


class ModelAdminMethodField:
    short_description: str
    admin_order_field: str


@admin.register(Task)
class TaskAdmin(BaseDjangoObjectActions, admin.ModelAdmin):
    list_display = ("name", "payload_unwraped", "status", "created_at", "updated_at")
    fields = readonly_fields = (
        "status",
        "name",
        "payload",
        "tags",
        "eta",
        "traceback_code",
    )
    list_filter = (TaskEventsFilter, "status")
    search_fields = ("name",)
    change_actions = actions = ("retry",)
    change_form_template = "admin/robust/task/change_form.html"

    def payload_unwraped(self, obj: Task) -> dict:
        return unwrap_payload(obj.payload)

    cast(ModelAdminMethodField, payload_unwraped).short_description = "payload"
    cast(ModelAdminMethodField, payload_unwraped).admin_order_field = "payload"

    def get_actions(self, request: HttpRequest) -> List[str]:
        actions = super(TaskAdmin, self).get_actions(request)
        if "delete_selected" in actions:
            del actions["delete_selected"]
        return actions

    def has_delete_permission(
        self, request: HttpRequest, obj: Optional[Task] = None
    ) -> bool:
        return False

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def traceback_code(self, task: Task) -> str:
        formatter = HtmlFormatter()
        lexer = Python3TracebackLexer()
        style = formatter.get_style_defs(".highlight")
        return mark_safe(
            "<style>{}</style><br/>{}".format(
                style, highlight(task.traceback, lexer, formatter)
            )
        )

    @takes_instance_or_queryset
    def retry(self, request: HttpRequest, qs: TaskQuerySet) -> None:
        count = 0
        for task in qs.filter(status=Task.FAILED):
            task.mark_retry()
            count += 1
        self.message_user(
            request,
            "{} tasks retried".format(count),
            messages.SUCCESS,
            fail_silently=True,
        )
