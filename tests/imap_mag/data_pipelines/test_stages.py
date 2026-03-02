"""Tests for Stages.py base classes."""

import pytest

import imap_mag.data_pipelines as dp
from imap_mag.data_pipelines.Stages import EndStage


class SimpleProcessingStage(dp.Stage):
    """Minimal Stage implementation for testing."""

    __test__ = False

    def __init__(self):
        super().__init__()
        self.processed_items: list[dp.Record] = []

    async def process(self, item: dp.Record, context: dict, **kwargs):
        self.processed_items.append(item)
        await self.publish_next(item, context, **kwargs)


class SimpleSourceStage(dp.SourceStage):
    """Minimal SourceStage implementation for testing."""

    __test__ = False

    def __init__(self, items: list[dp.Record] | None = None):
        super().__init__()
        self.items = items or [dp.Record(value="default")]

    async def start(self, context: dict, **kwargs):
        for item in self.items:
            await self.publish_next(item, context, **kwargs)


class ErrorThrowingStage(dp.Stage):
    """Stage that always raises on process."""

    __test__ = False

    async def process(self, item: dp.Record, context: dict, **kwargs):
        raise RuntimeError("Intentional test error")


class ErrorThrowingSourceStage(dp.SourceStage):
    """SourceStage that always raises on start."""

    __test__ = False

    async def start(self, context: dict, **kwargs):
        raise RuntimeError("Intentional source error")


# ---- Stage.prepare() ----


def test_prepare_raises_if_already_prepared():
    """Stage cannot be prepared twice."""
    stage = SimpleProcessingStage()
    end_stage = _make_end_stage()
    stage.prepare(dp.AutomaticRunParameters(), end_stage, index=1)

    with pytest.raises(
        RuntimeError, match="Stage has already been prepared with next stage"
    ):
        stage.prepare(dp.AutomaticRunParameters(), end_stage, index=2)


def test_prepare_sets_index_and_run_parameters():
    stage = SimpleProcessingStage()
    end_stage = _make_end_stage()
    params = dp.AutomaticRunParameters()

    returned_params = stage.prepare(params, end_stage, index=3)

    assert stage._index == 3
    assert stage._run_parameters is params
    assert stage._next_stage is end_stage
    assert returned_params is params


# ---- Stage.start() ----


@pytest.mark.asyncio
async def test_stage_start_calls_process():
    """Stage.start() should delegate to process() with a default init Record."""
    stage = SimpleProcessingStage()
    end_stage = _make_end_stage()
    stage.prepare(dp.AutomaticRunParameters(), end_stage, index=1)

    await stage.start(context={})

    # start() calls process() which calls publish_next() -> end_stage
    assert len(end_stage.results) == 1


# ---- Stage.publish_next() ----


@pytest.mark.asyncio
async def test_publish_next_raises_when_no_next_stage():
    """publish_next raises RuntimeError when there is no next stage configured."""
    stage = SimpleProcessingStage()
    # Do NOT call prepare() so _next_stage remains None

    with pytest.raises(
        RuntimeError, match="Cannot publish from a stage that has no next stage"
    ):
        await stage.publish_next(dp.Record(value="test"), context={})


# ---- Stage.stage_completed() ----


@pytest.mark.asyncio
async def test_stage_completed_propagates_to_next_stage():
    """stage_completed() should signal downstream stages."""
    pipeline = dp.Pipeline()
    source = SimpleSourceStage()
    processor = SimpleProcessingStage()

    pipeline.build(
        run_parameters=dp.AutomaticRunParameters(),
        stages=[source, processor],
    )
    await pipeline.run()

    assert pipeline.is_completed
    assert processor.is_completed


@pytest.mark.asyncio
async def test_stage_completed_only_runs_once():
    """stage_completed() should be idempotent - only runs completion logic once."""
    stage = SimpleProcessingStage()
    end_stage = _make_end_stage()
    stage.prepare(dp.AutomaticRunParameters(), end_stage, index=1)

    await stage.stage_completed(context={})
    assert stage.is_completed

    # Calling again should be idempotent
    await stage.stage_completed(context={})
    assert stage.is_completed


# ---- SourceStage.process() ----


@pytest.mark.asyncio
async def test_source_stage_process_raises_not_implemented():
    """SourceStage.process() should raise NotImplementedError."""
    stage = SimpleSourceStage()
    end_stage = _make_end_stage()
    stage.prepare(dp.AutomaticRunParameters(), end_stage, index=1)

    with pytest.raises(NotImplementedError):
        await stage.process(dp.Record(value="test"), context={})


# ---- EndStage.process() ----


@pytest.mark.asyncio
async def test_end_stage_process_raises_on_none_item():
    """EndStage.process() should raise ValueError when given a None item."""
    end_stage = _make_end_stage()

    with pytest.raises(ValueError, match="Cannot end pipeline with None"):
        await end_stage.process(None, context={})


@pytest.mark.asyncio
async def test_end_stage_collects_results():
    """EndStage.process() should collect items into results list."""
    end_stage = _make_end_stage()

    item1 = dp.Record(value="item1")
    item2 = dp.Record(value="item2")

    await end_stage.process(item1, context={})
    await end_stage.process(item2, context={})

    assert len(end_stage.results) == 2
    assert end_stage.results[0] is item1
    assert end_stage.results[1] is item2


# ---- EndStage.stage_completed() ----


@pytest.mark.asyncio
async def test_end_stage_stage_completed_marks_pipeline_complete():
    """EndStage.stage_completed() marks the parent pipeline as completed."""
    pipeline = dp.Pipeline()
    source = SimpleSourceStage()
    pipeline.build(
        run_parameters=dp.AutomaticRunParameters(),
        stages=[source],
    )
    await pipeline.run()

    assert pipeline.is_completed


# ---- Error logging in _log_stage_async ----


@pytest.mark.asyncio
async def test_stage_logs_error_when_process_raises(caplog):
    """Errors in process() are logged and re-raised."""
    import logging

    pipeline = dp.Pipeline()
    source = SimpleSourceStage(items=[dp.Record(value="test")])
    error_stage = ErrorThrowingStage()

    pipeline.build(
        run_parameters=dp.AutomaticRunParameters(),
        stages=[source, error_stage],
    )

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError, match="Intentional test error"):
            await pipeline.run()

    assert any("Error in process" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_source_stage_logs_error_when_start_raises(caplog):
    """Errors in start() are logged and re-raised."""
    import logging

    pipeline = dp.Pipeline()
    error_source = ErrorThrowingSourceStage()

    pipeline.build(
        run_parameters=dp.AutomaticRunParameters(),
        stages=[error_source],
    )

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError, match="Intentional source error"):
            await pipeline.run()

    assert any("Error in start" in record.message for record in caplog.records)


# ---- _LoggingABCMeta raises on sync method ----


def test_logging_abc_meta_raises_when_sync_process_method_defined():
    """_LoggingABCMeta should raise ValueError if a logged method is not async."""
    with pytest.raises(ValueError, match="not async"):

        class BadStage(dp.Stage):
            def process(self, item, context, **kwargs):  # sync instead of async
                pass


# ---- Helper ----


def _make_end_stage() -> EndStage:
    """Create a standalone EndStage with a mock parent pipeline."""
    pipeline = dp.Pipeline()

    # build a minimal pipeline to get a proper EndStage
    class _NullSource(dp.SourceStage):
        async def start(self, context, **kwargs):
            await self.publish_next(dp.Record(value="x"), context, **kwargs)

    pipeline.build(dp.AutomaticRunParameters(), stages=[_NullSource()])
    return pipeline._end_stage
