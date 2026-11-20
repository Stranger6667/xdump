from collections.abc import Callable, Sequence
from typing import Any, TypeVar

from ..base import BaseBackend

F = TypeVar("F", bound=Callable[..., Any])
Decorator = Callable[[Any], Any]


def apply_decorators(decorators: Sequence[Decorator]) -> Callable[[F], F]:
    """Apply multiple decorators to the same function. Useful for reusing common decorators among many functions."""

    def wrapper(func: F) -> F:
        for decorator in reversed(decorators):
            func = decorator(func)
        return func

    return wrapper


def import_backend(path: str) -> type[BaseBackend]:
    """Import a backend class by dotted path."""
    module_name, class_name = path.rsplit(".", 1)
    module = __import__(module_name, fromlist=[class_name])
    backend_class: type[BaseBackend] = getattr(module, class_name)
    return backend_class


def init_backend(backend_path: str, **kwargs: Any) -> BaseBackend:
    """Initialize a DB backend."""
    backend_class = import_backend(backend_path)
    return backend_class(**kwargs)
