# Make "import rollbar" import rollbar, and not the current module.
from __future__ import absolute_import

import json

import rollbar
from rollbar.logger import RollbarHandler


class StructlogRollbarHandler(RollbarHandler):
    def __init__(self, prefix, *args, **kwargs):
        """
        Structured rollbar handler. Rollbar messages are prefixed with the
        given prefix string. Any other arguments are passed to RollbarHandler.
        """
        self.prefix = prefix
        super(StructlogRollbarHandler, self).__init__(*args, **kwargs)

    def format_title(self, data):
        # Keys used to construct the title and for grouping purposes.
        KEYS = ["event", "func", "exception_name", "queue"]

        def format_field(field, value):
            if field == "queue":
                return "%s=%s" % (field, value.split(".")[0])
            else:
                return "%s=%s" % (field, value)

        return "%s: %s" % (
            self.prefix,
            " ".join(
                format_field(key, data[key]) for key in KEYS if key in data
            ),
        )

    def emit(self, record):
        level = record.levelname.lower()
        if level not in self.SUPPORTED_LEVELS:
            return

        if record.levelno < self.notify_level:
            return

        try:
            data = json.loads(record.msg)
        except json.JSONDecodeError:
            return super(StructlogRollbarHandler, self).emit(record)

        # Title and grouping
        data["title"] = data["fingerprint"] = self.format_title(data)

        uuid = rollbar.report_message(
            message=data.pop("traceback", data["title"]),
            level=level,
            request=rollbar.get_request(),
            extra_data={},
            payload_data=data,
        )

        if uuid:
            record.rollbar_uuid = uuid
