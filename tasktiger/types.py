import datetime
from typing import Any, Callable, Optional, Sequence, Tuple

RetryStrategy = Tuple[Callable[..., float], Tuple]
ScheduleStrategy = Tuple[Callable[..., Optional[datetime.datetime]], Tuple[Any, ...]]

# A retry strategy as stored with a task. The function is replaced with its
# import path, and the arguments come back as a list after a JSON round trip.
SerializedRetryStrategy = Tuple[str, Sequence[Any]]
