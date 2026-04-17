"""Regression tests for tasktiger.schedule.cron_expr.

Covers both the happy path (croniter + pytz installed) and the diagnostic
ImportError path that directs users to `pip install tasktiger[cron]`
"""

import datetime
import sys
from unittest.mock import patch

import pytest

from tasktiger.schedule import cron_expr


def test_cron_expr_happy_path() -> None:
    """With croniter + pytz available, cron_expr returns the next execution datetime."""
    fn, args = cron_expr("0 * * * *")
    result = fn(datetime.datetime(2026, 1, 1, 0, 30), *args)
    assert isinstance(result, datetime.datetime)
    assert result == datetime.datetime(2026, 1, 1, 1, 0)


@pytest.mark.parametrize("missing_module", ["croniter", "pytz"])
def test_cron_expr_missing_dependency_error_message(missing_module: str) -> None:
    """Missing croniter or pytz raises ImportError naming tasktiger[cron]."""
    fn, args = cron_expr("0 * * * *")
    # Setting the module to None in sys.modules makes `import <name>` raise
    # ModuleNotFoundError even if the package is installed in the env.
    with patch.dict(sys.modules, {missing_module: None}):
        with pytest.raises(ImportError) as exc_info:
            fn(datetime.datetime(2026, 1, 1, 0, 30), *args)
    message = str(exc_info.value)
    assert "pip install tasktiger[cron]" in message
    assert missing_module in message
