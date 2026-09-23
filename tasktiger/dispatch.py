from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class TaskDispatch:
    """A resolved task handler with execution metadata."""

    handler: Callable[..., Any]
    batch: bool = False

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.handler(*args, **kwargs)
