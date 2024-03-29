import re

REDIS_GLOB_CHARACTER_PATTERN = re.compile(r"([\\?*\[\]])")


def redis_glob_escape(value: str) -> str:
    return REDIS_GLOB_CHARACTER_PATTERN.sub(r"\\\1", value)
