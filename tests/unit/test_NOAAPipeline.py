"""Tests for NOAAPipeline."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from imap_mag.data_pipelines import AutomaticRunParameters
from imap_mag.data_pipelines.NOAAPipeline import NOAAPipeline

_INIT_PATCHES = (
    "imap_mag.data_pipelines.NOAAPipeline.NOAARTSWApiClient",
    "imap_mag.data_pipelines.NOAAPipeline.FileFinder",
    "imap_mag.data_pipelines.NOAAPipeline.FetchNOAA",
)
_BUILD_PATCHES = (
    "imap_mag.data_pipelines.NOAAPipeline.DownloadNOAAStage",
    "imap_mag.data_pipelines.NOAAPipeline.PublishFileToDatastoreStage",
)


def _make_settings(
    url: str = "https://example.org/noaa",
    publish_to_data_store: bool = True,
    work_folder: Path = Path("/tmp/work"),
) -> MagicMock:
    """Return a minimal mock AppSettings for NOAAPipeline."""
    mock_settings = MagicMock()
    mock_settings.fetch_solar1_ace.api.url_base = url
    mock_settings.fetch_solar1_ace.publish_to_data_store = publish_to_data_store
    mock_settings.setup_work_folder_for_command.return_value = work_folder
    return mock_settings


class TestNOAAPipelineInit:
    """Tests for NOAAPipeline.__init__."""

    def test_progress_item_name_uppercases_spacecraft_and_instrument(self) -> None:
        with patch(_INIT_PATCHES[0]), patch(_INIT_PATCHES[1]), patch(_INIT_PATCHES[2]):
            pipeline = NOAAPipeline(
                spacecraft="solar1",
                instrument="mag",
                database=None,
                settings=_make_settings(),
            )

        assert pipeline.initial_context["progress_item_name"] == "SOLAR1_MAG"

    def test_constructs_api_client_with_url_from_settings(self) -> None:
        mock_settings = _make_settings(url="https://example.org/noaa")

        with (
            patch(_INIT_PATCHES[0]) as mock_client_cls,
            patch(_INIT_PATCHES[1]),
            patch(_INIT_PATCHES[2]),
        ):
            NOAAPipeline(
                spacecraft="SOLAR1",
                instrument="mag",
                database=None,
                settings=mock_settings,
            )

        mock_client_cls.assert_called_once_with("https://example.org/noaa")

    def test_constructs_fetcher_with_client_work_folder_and_datastore_finder(
        self,
    ) -> None:
        work_folder = Path("/tmp/work")
        mock_settings = _make_settings(work_folder=work_folder)

        with (
            patch(_INIT_PATCHES[0]) as mock_client_cls,
            patch(_INIT_PATCHES[1]) as mock_finder_cls,
            patch(_INIT_PATCHES[2]) as mock_fetch_cls,
        ):
            NOAAPipeline(
                spacecraft="SOLAR1",
                instrument="mag",
                database=None,
                settings=mock_settings,
            )

        mock_fetch_cls.assert_called_once_with(
            data_access=mock_client_cls.return_value,
            work_folder=work_folder,
            datastore_finder=mock_finder_cls.return_value,
        )


class TestNOAAPipelineBuild:
    """Tests for NOAAPipeline.build."""

    def _make_pipeline(
        self,
        spacecraft: str = "SOLAR1",
        instrument: str = "mag",
        database=None,
        settings: MagicMock | None = None,
    ) -> NOAAPipeline:
        """Construct a NOAAPipeline with all __init__ dependencies mocked."""
        with patch(_INIT_PATCHES[0]), patch(_INIT_PATCHES[1]), patch(_INIT_PATCHES[2]):
            return NOAAPipeline(
                spacecraft=spacecraft,
                instrument=instrument,
                database=database,
                settings=settings or _make_settings(),
            )

    def test_build_sets_run_parameters(self) -> None:
        pipeline = self._make_pipeline()

        with patch(_BUILD_PATCHES[0]), patch(_BUILD_PATCHES[1]):
            pipeline.build(AutomaticRunParameters())

        assert pipeline._run_parameters is not None

    def test_build_download_stage_receives_spacecraft_instrument_and_fetcher(
        self,
    ) -> None:
        pipeline = self._make_pipeline(spacecraft="ACE", instrument="plasma")

        with (
            patch(_BUILD_PATCHES[0]) as mock_download_cls,
            patch(_BUILD_PATCHES[1]),
        ):
            pipeline.build(AutomaticRunParameters())

        mock_download_cls.assert_called_once_with(
            spacecraft="ACE",
            instrument="plasma",
            fetcher=pipeline._fetcher,
        )

    def test_build_publish_stage_enabled_when_publish_to_data_store_is_true(
        self,
    ) -> None:
        pipeline = self._make_pipeline(
            settings=_make_settings(publish_to_data_store=True)
        )

        with (
            patch(_BUILD_PATCHES[0]),
            patch(_BUILD_PATCHES[1]) as mock_publish_cls,
        ):
            pipeline.build(AutomaticRunParameters())

        assert mock_publish_cls.call_args.kwargs["enabled"] is True

    def test_build_publish_stage_disabled_when_publish_to_data_store_is_false(
        self,
    ) -> None:
        pipeline = self._make_pipeline(
            settings=_make_settings(publish_to_data_store=False)
        )

        with (
            patch(_BUILD_PATCHES[0]),
            patch(_BUILD_PATCHES[1]) as mock_publish_cls,
        ):
            pipeline.build(AutomaticRunParameters())

        assert mock_publish_cls.call_args.kwargs["enabled"] is False

    def test_build_stages_are_download_then_publish(self) -> None:
        pipeline = self._make_pipeline()

        with (
            patch(_BUILD_PATCHES[0]) as mock_download_cls,
            patch(_BUILD_PATCHES[1]) as mock_publish_cls,
        ):
            pipeline.build(AutomaticRunParameters())

        assert pipeline._stages[0] is mock_download_cls.return_value
        assert pipeline._stages[1] is mock_publish_cls.return_value
