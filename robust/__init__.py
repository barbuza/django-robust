from typing import Optional, List, Type, Callable

from .exceptions import Retry

if False:
    from .models import TaskWrapper

__all__ = ('task', 'Retry')


def task(bind: bool = False, tags: Optional[List[str]] = None,
         retries: Optional[int] = None) \
        -> Callable[['function'], Type['TaskWrapper']]:
    from .models import task as models_task
    return models_task(bind=bind, tags=tags, retries=retries)


default_app_config = 'robust.apps.RobustConfig'
