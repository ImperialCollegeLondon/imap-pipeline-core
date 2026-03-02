from dataclasses import dataclass
from typing import Generic, TypeVar

from imap_mag.data_pipelines.Record import Record

T = TypeVar("T", bound=Record)


@dataclass
class Result(Generic[T]):
    def __init__(
        self,
        success: bool,
        data_items: list[T] | None = None,
        data_dict: dict | None = None,
    ):
        self.success = success
        self.data_items = data_items or []
        self.data_dict = data_dict or {}

    @staticmethod
    def create_success(
        data_items: list[T] | None = None, data_dict: dict | None = None
    ) -> "Result[T]":
        return Result(success=True, data_items=data_items, data_dict=data_dict)

    @staticmethod
    def create_failure(
        data_items: list[T] | None = None, data_dict: dict | None = None
    ) -> "Result[T]":
        return Result(success=False, data_items=data_items, data_dict=data_dict)
