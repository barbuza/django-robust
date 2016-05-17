from django.contrib import admin

from .models import Task


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ('name', 'payload', 'status', 'created_at', 'updated_at')
    fields = readonly_fields = ('status', 'name', 'payload', 'tags', 'eta')
    list_filter = ('status',)
    actions = None

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False
