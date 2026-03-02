from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class Record:
    def __init__(self, value=None, **kwargs):
        if value is None and not kwargs:
            raise ValueError(
                "Record must have at least a value or some additional attributes"
            )
        if value is not None:
            self.value = value
        for key, val in kwargs.items():
            setattr(self, key, val)

    def __repr__(self):
        # return Record( with KV dictionary of all non none attibutes for easier logging and debugging
        attrs = {k: v for k, v in self.__dict__.items() if v is not None}
        # remove quotes from string values for cleaner logging
        attrs = {
            k: (v if not isinstance(v, str) else v.strip('"')) for k, v in attrs.items()
        }
        # remove curly braces from the dict for cleaner logging
        attrs = ", ".join(f"{k}={v}" for k, v in attrs.items())
        return f"Record({attrs})"


class FileRecord(Record):
    def __init__(self, file_path: Path, content_date: datetime):
        super().__init__(value=file_path.name, content_date=content_date)
        self.file_path = file_path
