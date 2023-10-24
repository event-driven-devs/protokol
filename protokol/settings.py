import json
import os


def get_boolean(key: str) -> bool:
    return os.getenv(key, "").lower() in ("true", "t", "yes", "y", "1")


DEBUG = get_boolean("DEBUG")
DEFAULT_CALL_TIMEOUT: float = 5.0


loads = json.loads
dumps = json.dumps
