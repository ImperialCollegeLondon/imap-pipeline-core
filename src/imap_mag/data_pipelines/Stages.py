import asyncio
import functools
import logging
from abc import ABC, ABCMeta, abstractmethod
from typing import ClassVar

from imap_mag.data_pipelines.Record import Record
from imap_mag.data_pipelines.RunParameters import PipelineRunParameters


def _log_stage(func):
    """Decorator that logs the start and end of abstract method implementations."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        method_name = func.__name__
        class_name = args[0].__class__.__name__ if args else "Unknown"
        class_module_name = args[0].__class__.__module__

        # do not log if this is EndStage
        if (
            class_name == "EndStage"
            and class_module_name == "imap_mag.data_pipelines.Stages"
        ):
            return func(*args, **kwargs)

        logger = logging.getLogger(f"{class_module_name}.{class_name}")
        index = getattr(args[0], "_index", None) if len(args) > 0 else None
        log_prefix = f"Stage {index}: " if index is not None else ""

        # remove the self argument from the logged args for cleaner logging
        logged_args = str(args[1:] if len(args) > 1 else []).strip(
            "()[]"
        )  # remove parentheses from single argument tuple

        # log kwargs if there are any, but remove the curly braces from the dict for cleaner logging
        kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        if logged_args and kwargs_str:
            kwargs_str = f" {kwargs_str}"

        logger.info(f"{log_prefix}{method_name}({logged_args}{kwargs_str})")
        try:
            result = func(*args, **kwargs)
            logger.info(f"{log_prefix} {method_name} completed")
            return result
        except Exception as e:
            logger.error(f"{log_prefix}Error in {method_name}", exc_info=e)
            raise

    return wrapper


class _LoggingABCMeta(ABCMeta):
    """Metaclass that automatically applies logging decorator to method implementations including abstract methods."""

    # make this static so it can be used in the __new__ method without needing an instance of the metaclass
    logged_method_names: ClassVar[list[str]] = ["start", "process", "stage_completed"]

    def __new__(mcs, name, bases, namespace, **kwargs):
        for attr_name, attr_value in namespace.items():
            # if attr_name in abstract_methods and callable(attr_value):
            if attr_name in _LoggingABCMeta.logged_method_names and callable(
                attr_value
            ):
                namespace[attr_name] = _log_stage(attr_value)

        return super().__new__(mcs, name, bases, namespace, **kwargs)


class Stage(ABC, metaclass=_LoggingABCMeta):
    def __init__(self):
        self.is_completed = False
        self._next_stage: Stage | None = None
        self._run_parameters: PipelineRunParameters | None = None
        self._index: int = 0

        # create a logger that is specific to the derived class that is being instantiated, so that the logs will show the correct class name even when the logging decorator is defined in the base class
        self.logger = logging.getLogger(
            f"{self.__class__.__module__}.{self.__class__.__name__}"
        )

    def prepare(
        self,
        run_parameters: PipelineRunParameters,
        next_stage: "Stage | None",
        index: int,
    ) -> PipelineRunParameters:
        if self._next_stage is not None:
            raise RuntimeError("Stage has already been prepared with next stage.")

        self._run_parameters = run_parameters
        self._next_stage = next_stage
        self._index = index
        return run_parameters

    async def start(self, context: dict, **kwargs):
        # Convenience method to start the stage without needing to pass an initial Record - it will just call process() with None as the item
        return await self.process(Record("init"), context=context, **kwargs)

    @abstractmethod
    async def process(self, item: Record, context: dict, **kwargs):
        pass

    async def publish_next(self, item: Record, context: dict, **kwargs):
        # This method can be used by stages to publish data items to the next stage in the pipeline
        if self._next_stage is None:
            raise RuntimeError("Cannot publish from a stage that has no next stage.")

        return await self._next_stage.process(item, context=context, **kwargs)

    async def stage_completed(self, context: dict):
        # The previous stage is done, no more items will be published to this stage
        # do any finalization logic and then signal to the next stage that this stage is completed
        if not self.is_completed:
            self.is_completed = True
            if self._next_stage:
                await self._next_stage.stage_completed(context)


class SourceStage(Stage):
    @abstractmethod
    async def start(self, context: dict, **kwargs):
        # This method is called to start the pipeline run and should publish the first items to the next stage
        pass

    async def process(self, item: Record, context: dict, **kwargs):
        # Source stage does not process any items, it only starts the pipeline by publishing the first items in the start() method
        raise NotImplementedError(
            "Source stage does not implement process() method - it should publish initial items in start() method."
        )


class EndStage(Stage):
    from imap_mag.data_pipelines import Pipeline

    def __init__(self, parent_pipeline: "Pipeline", index: int):
        super().__init__()
        self.parent_pipeline = parent_pipeline
        self.results: list[Record] = []
        self.args: dict | None = None
        self._index = index

    async def process(self, item: Record, context: dict, **kwargs):
        if not item:
            raise ValueError("Cannot end pipeline with None")

        # This stage does not yield any data items, it just collects any results
        self.args = kwargs
        self.context = context
        self.results.append(item)
        await asyncio.sleep(0)

    async def stage_completed(self, context: dict):
        if not self.is_completed:
            self.is_completed = True
            self.parent_pipeline._completed()
            await asyncio.sleep(0)
