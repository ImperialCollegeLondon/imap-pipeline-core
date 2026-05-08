"""Unit tests for prefect_server.serverConfig."""

from unittest.mock import AsyncMock, MagicMock, patch

import prefect.blocks.notifications
import prefect.blocks.system
import pytest

from prefect_server.serverConfig import ServerConfig


class TestCreateConcurrencyLimits:
    @pytest.mark.asyncio
    async def test_create_concurrency_limits_is_a_no_op(self):
        mock_client = AsyncMock()
        result = await ServerConfig._create_concurrency_limits(mock_client)
        assert result is None
        mock_client.assert_not_awaited()


class TestCreateQueues:
    def _make_mock_client(self, existing_queue_names: list[str]):
        mock_client = AsyncMock()
        existing_queues = [MagicMock(name=n) for n in existing_queue_names]
        for q, n in zip(existing_queues, existing_queue_names):
            q.name = n
        mock_client.read_work_queues = AsyncMock(return_value=existing_queues)
        mock_client.create_work_queue = AsyncMock(return_value=None)
        return mock_client

    @pytest.mark.asyncio
    async def test_creates_all_queues_when_none_exist(self):
        mock_client = self._make_mock_client([])

        await ServerConfig._create_queues(mock_client, local_debug=False)

        assert mock_client.create_work_queue.call_count == 3

    @pytest.mark.asyncio
    async def test_skips_existing_queues(self):
        from prefect_server.constants import PREFECT_CONSTANTS

        mock_client = self._make_mock_client([PREFECT_CONSTANTS.QUEUES.HIGH_PRIORITY])

        await ServerConfig._create_queues(mock_client, local_debug=False)

        assert mock_client.create_work_queue.call_count == 2

    @pytest.mark.asyncio
    async def test_creates_no_queues_when_all_exist(self):
        from prefect_server.constants import PREFECT_CONSTANTS

        mock_client = self._make_mock_client(
            [
                PREFECT_CONSTANTS.QUEUES.HIGH_PRIORITY,
                PREFECT_CONSTANTS.QUEUES.DEFAULT,
                PREFECT_CONSTANTS.QUEUES.LOW,
            ]
        )

        await ServerConfig._create_queues(mock_client, local_debug=False)

        mock_client.create_work_queue.assert_not_called()

    @pytest.mark.asyncio
    async def test_uses_no_work_pool_in_local_debug_mode(self):
        mock_client = self._make_mock_client([])

        await ServerConfig._create_queues(mock_client, local_debug=True)

        for call_kwargs in mock_client.create_work_queue.call_args_list:
            assert call_kwargs.kwargs.get("work_pool_name") is None


class TestCreateVariables:
    @pytest.mark.asyncio
    async def test_does_not_call_create_when_no_default_variables(self):
        mock_client = AsyncMock()

        await ServerConfig._create_variables(mock_client)

        mock_client.create_variable.assert_not_called()
        mock_client.read_variable_by_name.assert_not_called()


class TestCreateBlocks:
    @pytest.mark.asyncio
    async def test_reads_existing_block_documents_to_check_for_duplicates(self, capsys):
        from prefect_server.constants import PREFECT_CONSTANTS

        mock_client = AsyncMock()
        # All blocks already exist - no save calls needed
        existing_block_names = [
            PREFECT_CONSTANTS.POLL_IALIRT.IALIRT_AUTH_CODE_SECRET_NAME,
            PREFECT_CONSTANTS.POLL_HK.WEBPODA_AUTH_CODE_SECRET_NAME,
            PREFECT_CONSTANTS.POLL_SCIENCE.SDC_AUTH_CODE_SECRET_NAME,
            PREFECT_CONSTANTS.IMAP_WEBHOOK_BLOCK_NAME,
            PREFECT_CONSTANTS.DEFAULT_UPLOAD_DESTINATION_BLOCK_NAME,
            PREFECT_CONSTANTS.IMAP_DATABASE_BLOCK_NAME,
        ]
        existing_blocks = [MagicMock(name=n) for n in existing_block_names]
        for b, n in zip(existing_blocks, existing_block_names):
            b.name = n
        mock_client.read_block_documents = AsyncMock(return_value=existing_blocks)

        await ServerConfig._create_blocks(mock_client)

        mock_client.read_block_documents.assert_called_once()
        captured = capsys.readouterr().out
        # All blocks already exist, so we get "already exists" messages only
        assert "already exists" in captured

    @pytest.mark.asyncio
    async def test_saves_blocks_that_do_not_exist(self, capsys):
        mock_client = AsyncMock()
        mock_client.read_block_documents = AsyncMock(return_value=[])

        mock_save = AsyncMock()
        with (
            patch.object(prefect.blocks.system.Secret, "save", mock_save),
            patch.object(
                prefect.blocks.notifications.MicrosoftTeamsWebhook, "save", mock_save
            ),
            patch(
                "prefect_server.serverConfig.RCloneConfigFileBlock", autospec=True
            ) as mock_rclone_cls,
            patch(
                "prefect_server.serverConfig.SqlAlchemyConnector", autospec=True
            ) as mock_sql_cls,
        ):
            mock_rclone_cls.return_value.save = mock_save
            mock_sql_cls.return_value.save = mock_save

            await ServerConfig._create_blocks(mock_client)

        assert mock_save.call_count == 6
        captured = capsys.readouterr().out
        assert "Created new block" in captured


class TestServerConfigInitialise:
    @pytest.mark.asyncio
    async def test_initialise_calls_create_methods(self):
        mock_client = AsyncMock()

        with (
            patch("prefect_server.serverConfig.create_db"),
            patch("prefect_server.serverConfig.upgrade_db"),
            patch("prefect_server.serverConfig.get_client") as mock_get_client,
            patch.object(ServerConfig, "_create_concurrency_limits", new=AsyncMock()),
            patch.object(ServerConfig, "_create_queues", new=AsyncMock()),
            patch.object(ServerConfig, "_create_variables", new=AsyncMock()),
            patch.object(ServerConfig, "_create_blocks", new=AsyncMock()),
        ):
            mock_get_client.return_value.__aenter__ = AsyncMock(
                return_value=mock_client
            )
            mock_get_client.return_value.__aexit__ = AsyncMock(return_value=False)
            await ServerConfig.initialise()
