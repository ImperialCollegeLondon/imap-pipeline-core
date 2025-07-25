import os
from contextlib import AbstractContextManager


class Environment(AbstractContextManager):
    """Context to manage environment variables."""

    key_values: dict[str, str]

    def __init__(self, *args, **kwargs) -> None:
        self.key_values = {}

        for i in range(len(args) // 2):
            self.key_values[args[i * 2]] = args[i * 2 + 1]

        self.key_values.update(kwargs)
        self.original_values = {key: os.environ.get(key) for key in self.key_values}

    def __enter__(self):
        """Set the environment variables."""
        os.environ.update(self.key_values)

    def __exit__(self, exc_type, exc_value, traceback):
        """Restore the original environment variables."""

        for key, value in self.original_values.items():
            if value is None:
                del os.environ[key]
            else:
                os.environ[key] = value
