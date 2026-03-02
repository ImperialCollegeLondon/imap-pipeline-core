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
