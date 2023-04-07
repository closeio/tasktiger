from typing import Callable, Tuple

RetryStrategy = Tuple[Callable[..., float], Tuple]
