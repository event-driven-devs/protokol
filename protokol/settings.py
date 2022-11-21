import os


def get_boolean(key: str) -> bool:
    return os.getenv(key, "").lower() in ("true", "t", "yes", "y", "1")


DEBUG = get_boolean("DEBUG")
CALL_TIMEOUT = 5
