from typing import TYPE_CHECKING, Any, Callable, List, Optional, Type

from .exceptions import Retry

if TYPE_CHECKING:
    from .models import TaskWrapper

__all__ = ("task", "Retry", "clear_transaction_context")


def task(
    bind: bool = False, tags: Optional[List[str]] = None, retries: Optional[int] = None
) -> Callable[[Callable[..., Any]], Type["TaskWrapper"]]:
    from .models import task as models_task

    return models_task(bind=bind, tags=tags, retries=retries)


def clear_transaction_context() -> None:
    from .models import TRANSACTION_CONTEXT

    TRANSACTION_CONTEXT.clear()


default_app_config = "robust.apps.RobustConfig"
