from django.contrib import admin
from django.contrib import messages
from django_object_actions import BaseDjangoObjectActions, takes_instance_or_queryset

from .models import Task


@admin.register(Task)
class TaskAdmin(BaseDjangoObjectActions, admin.ModelAdmin):
    list_display = ('name', 'payload', 'status', 'created_at', 'updated_at')
    fields = readonly_fields = ('status', 'name', 'payload', 'tags', 'eta')
    list_filter = ('status',)
    change_actions = actions = ('retry',)

    def get_actions(self, request):
        actions = super(TaskAdmin, self).get_actions(request)
        del actions['delete_selected']
        return actions

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False

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
