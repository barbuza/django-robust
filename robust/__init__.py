from typing import Optional, List, Callable, cast, Any

from .exceptions import Retry

__all__ = ('task', 'Retry')


def task(bind: bool = False, tags: Optional[List[str]] = None,
         retries: Optional[int] = None) -> Callable[['function'], 'function']:
    from .models import task as models_task
    return cast(Any, models_task(bind=bind, tags=tags, retries=retries))


default_app_config = 'robust.apps.RobustConfig'
