import datetime

__all__ = ["periodic"]


def _periodic(dt, period, start_date, end_date):
    if end_date and dt >= end_date:
        return None

    if dt < start_date:
        return start_date

    # Determine the next time the task should be run
    delta = dt - start_date
    seconds = delta.seconds + delta.days * 86400
    runs = seconds // period
    next_run = runs + 1
    next_date = start_date + datetime.timedelta(seconds=next_run * period)

    # Make sure the time is still within bounds.
    if end_date and next_date > end_date:
        return None

    return next_date


def periodic(
    seconds=0,
    minutes=0,
    hours=0,
    days=0,
    weeks=0,
    start_date=None,
    end_date=None,
):
    """
    Periodic task schedule: Use to schedule a task to run periodically,
    starting from start_date (or None to be active immediately) until end_date
    (or None to repeat forever).

    The period starts at the given start_date, or on Jan 1st 2000.

    For more details, see README.
    """
    period = (
        seconds + minutes * 60 + hours * 3600 + days * 86400 + weeks * 604800
    )
    assert period > 0, "Must specify a positive period."
    if not start_date:
        # Saturday at midnight
        start_date = datetime.datetime(2000, 1, 1)
    return (_periodic, (period, start_date, end_date))
