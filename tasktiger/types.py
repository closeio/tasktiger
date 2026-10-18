import datetime
from typing import Any, Callable, Optional, Tuple

RetryStrategy = Tuple[Callable[..., float], Tuple]
ScheduleStrategy = Tuple[Callable[..., Optional[datetime.datetime]], Tuple[Any, ...]]
