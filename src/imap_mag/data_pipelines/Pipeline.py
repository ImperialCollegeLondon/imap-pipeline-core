import logging

from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines.Result import Result
from imap_mag.data_pipelines.RunParameters import (
    AutomaticRunParameters,
    PipelineRunParameters,
)
from imap_mag.data_pipelines.Stages import EndStage, Stage
from imap_mag.util import DatetimeProvider

logger = logging.getLogger(__name__)


class Pipeline:
    STARTED_CONTEXT_KEY = "started"

    def __init__(self, settings: AppSettings = AppSettings()):
        self._settings = settings
        self._run_parameters: PipelineRunParameters | None = None
        self.is_running = False
        self.is_completed = False
        self._stages: list[Stage] = []
        self._first_stage: Stage | None = None
        self.initial_context: dict = {}

    def build(
        self,
        run_parameters: PipelineRunParameters = AutomaticRunParameters(),
        stages: list[Stage] | None = None,
    ):
        if self.is_running:
            raise RuntimeError("Cannot build pipeline while it is running.")

        if not stages or len(stages) == 0:
            raise ValueError("Pipeline must have at least one stage.")

        self._run_parameters = run_parameters

        # we add a EndStage at the end of the stages provided by the user to handle finalization logic and results collection
        self._end_stage = EndStage(parent_pipeline=self, index=len(stages) + 1)
        self._stages = stages
        next_params = run_parameters
        for i in range(len(stages)):
            next_stage = stages[i + 1] if i < len(stages) - 1 else self._end_stage
            # Each stage can modify the run parameters for the next stage based on its own logic and the parameters it received
            next_params = stages[i].prepare(next_params, next_stage, index=i + 1)

    async def run(self):
        if self._run_parameters is None:
            raise ValueError(
                "Pipeline run parameters not set. Call build() before run()."
            )

        self.is_running = True
        first_stage = self._stages[0]
        start_time = DatetimeProvider.now()
        pipeline_context = {
            self.STARTED_CONTEXT_KEY: start_time,
        } | self.initial_context  # merge with any initial context provided

        await first_stage.start(context=pipeline_context)
        await first_stage.stage_completed(context=pipeline_context)

        self.is_running = False

        # each stage will have called later stage one or more times

        if not self.is_completed:
            raise RuntimeError(
                "Pipeline run completed but pipeline is not marked as completed. This likely means the final stage was not a EndStage or the final stage did not publish a final item."
            )

        logger.info(
            f"Pipeline completed in {DatetimeProvider.now() - start_time}. end context: {pipeline_context}"
        )

    def _completed(self):
        self.is_completed = True

    def get_results(self) -> Result:
        if not self.is_completed:
            raise RuntimeError(
                "Cannot get results from pipeline that has not completed."
            )

        end_stage = self._end_stage
        if not isinstance(end_stage, EndStage):
            raise RuntimeError("Last stage of pipeline is not a EndStage.")

        # TODO - think about failure
        return Result.create_success(
            data_items=end_stage.results, data_dict=end_stage.args
        )
