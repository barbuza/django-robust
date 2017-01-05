from django.contrib import admin
from django.contrib import messages
from django.utils.safestring import mark_safe
from django_object_actions import BaseDjangoObjectActions, takes_instance_or_queryset
from pygments import highlight
from pygments.formatters import get_formatter_by_name
from pygments.lexers import get_lexer_by_name
import sys

from .models import Task
from .utils import unwrap_payload


class TaskEventsFilter(admin.SimpleListFilter):
    SUCCEED = 'succeed'
    TROUBLED = 'troubled'

    title = parameter_name = 'events'

    def lookups(self, request, model_admin):
        return [
            (self.SUCCEED, 'Succeed'),
            (self.TROUBLED, 'Troubled'),
        ]

    def queryset(self, request, queryset):
        if self.value() == self.TROUBLED:
            queryset = queryset.with_fails()
        elif self.value() == self.SUCCEED:
            queryset = queryset.without_fails()
        return queryset


@admin.register(Task)
class TaskAdmin(BaseDjangoObjectActions, admin.ModelAdmin):
    list_display = ('name', 'payload_unwraped', 'status', 'created_at', 'updated_at')
    fields = readonly_fields = ('status', 'name', 'payload', 'tags', 'eta', 'traceback_code')
    list_filter = (TaskEventsFilter, 'status')
    search_fields = ('name',)
    change_actions = actions = ('retry',)
    change_form_template = 'admin/robust/task/change_form.html'

    def payload_unwraped(self, obj):
        return unwrap_payload(obj.payload)

    payload_unwraped.short_description = 'payload'
    payload_unwraped.admin_order_field = 'payload'

    def get_actions(self, request):
        actions = super(TaskAdmin, self).get_actions(request)
        del actions['delete_selected']
        return actions

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False

    def traceback_code(self, task):
        formatter = get_formatter_by_name('html')
        if sys.version_info.major == 3:
            lexer = get_lexer_by_name('py3tb')
        else:
            lexer = get_lexer_by_name('pytb')
        style = formatter.get_style_defs('.highlight')
        return mark_safe('<style>{}</style><br/>{}'.format(style, highlight(task.traceback, lexer, formatter)))

    @takes_instance_or_queryset
    def retry(self, request, qs):
        count = 0
        for task in qs.filter(status=Task.FAILED):
            task.mark_retry()
            count += 1
        self.message_user(
            request, '{} tasks retried'.format(count),
            messages.SUCCESS, fail_silently=True
        )
