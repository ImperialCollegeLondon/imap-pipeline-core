import logging

import pytest

import imap_mag.data_pipelines as dp

logging.getLogger(__name__).setLevel(logging.DEBUG)


class TestSourceStage(dp.SourceStage):
    __test__ = False  # prevent pytest collecting this as a test

    async def start(self, context: dict, **kwargs):
        await self.publish_next(
            dp.Record(name="item1", value="value1"), context, **kwargs
        )
        await self.publish_next(
            dp.Record(name="item2", value="value2", magic="wand"), context, **kwargs
        )


class TestProcessingStage(dp.Stage):
    __test__ = False  # prevent pytest collecting this as a test

    def __init__(self):
        super().__init__()
        self.counter = 0

    async def process(self, item: dp.Record | None, context: dict, **kwargs):
        self.counter += 1
        processed_value = f"{item.value}_{self.counter}"
        await self.publish_next(
            dp.Record(name=item.name, value=processed_value), context, **kwargs
        )


@pytest.mark.asyncio
async def test_autorun_pipeline_can_be_created_and_processes_depth_first() -> None:
    pipeline = dp.Pipeline()

    pipeline.build(
        run_parameters=dp.AutomaticRunParameters(),
        stages=[
            TestSourceStage(),
            TestProcessingStage(),
            TestProcessingStage(),
        ],
    )

    assert pipeline is not None

    await pipeline.run()

    assert pipeline.is_completed
    results = pipeline.get_results()
    assert len(results.data_items) == 2
    assert results.success
    assert results.data_items[0].name == "item1"
    assert results.data_items[0].value == "value1_1_1"
    assert results.data_items[1].name == "item2"
    assert results.data_items[1].value == "value2_2_2"


def test_build_raises_when_pipeline_is_already_running():
    pipeline = dp.Pipeline()
    pipeline.build(
        run_parameters=dp.AutomaticRunParameters(),
        stages=[TestSourceStage()],
    )
    pipeline.is_running = True

    with pytest.raises(RuntimeError, match="Cannot build pipeline while it is running"):
        pipeline.build(
            run_parameters=dp.AutomaticRunParameters(),
            stages=[TestSourceStage()],
        )


def test_build_raises_when_no_stages_provided():
    pipeline = dp.Pipeline()

    with pytest.raises(ValueError, match="Pipeline must have at least one stage"):
        pipeline.build(
            run_parameters=dp.AutomaticRunParameters(),
            stages=[],
        )


def test_build_raises_when_stages_is_none():
    pipeline = dp.Pipeline()

    with pytest.raises(ValueError, match="Pipeline must have at least one stage"):
        pipeline.build(
            run_parameters=dp.AutomaticRunParameters(),
            stages=None,
        )


@pytest.mark.asyncio
async def test_run_raises_when_not_built():
    pipeline = dp.Pipeline()

    with pytest.raises(ValueError, match="Pipeline run parameters not set"):
        await pipeline.run()


def test_get_results_raises_when_not_completed():
    pipeline = dp.Pipeline()
    pipeline.build(
        run_parameters=dp.AutomaticRunParameters(),
        stages=[TestSourceStage()],
    )

    with pytest.raises(
        RuntimeError, match="Cannot get results from pipeline that has not completed"
    ):
        pipeline.get_results()


@pytest.mark.asyncio
async def test_pipeline_merges_initial_context():
    """Test that initial_context values are available to stages during run."""
    received_context = {}

    class ContextCapturingStage(dp.SourceStage):
        async def start(self, context: dict, **kwargs):
            received_context.update(context)
            await self.publish_next(dp.Record(value="done"), context, **kwargs)

    pipeline = dp.Pipeline()
    pipeline.initial_context = {"custom_key": "custom_value"}
    pipeline.build(
        run_parameters=dp.AutomaticRunParameters(),
        stages=[ContextCapturingStage()],
    )
    await pipeline.run()

    assert received_context.get("custom_key") == "custom_value"
    assert dp.Pipeline.STARTED_CONTEXT_KEY in received_context
