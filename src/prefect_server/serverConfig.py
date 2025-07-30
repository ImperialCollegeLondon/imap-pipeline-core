import prefect
import prefect.docker
from prefect import get_client
from pydantic import SecretStr

from imap_db.main import create_db, upgrade_db
from prefect_server.constants import PREFECT_CONSTANTS


class ServerConfig:
    @staticmethod
    async def initialise(local_debug: bool = False):
        # Create IMAP database
        create_db()
        upgrade_db()

        # Initialize server configuration
        async with get_client() as client:
            await ServerConfig._create_concurrency_limits(client)
            await ServerConfig._create_queues(client, local_debug)
            await ServerConfig._create_variables(client)
            await ServerConfig._create_blocks(client)

    @staticmethod
    async def _create_concurrency_limits(client):
        pass

    @staticmethod
    async def _create_queues(client, local_debug: bool):
        existing_queues = await client.read_work_queues()

        work_pool = PREFECT_CONSTANTS.DEFAULT_WORKPOOL if not local_debug else None

        if PREFECT_CONSTANTS.QUEUES.HIGH_PRIORITY not in [
            q.name for q in existing_queues
        ]:
            await client.create_work_queue(
                name=PREFECT_CONSTANTS.QUEUES.HIGH_PRIORITY,
                concurrency_limit=1,
                priority=1,
                work_pool_name=work_pool,
            )
            print(f"Created new work queue '{PREFECT_CONSTANTS.QUEUES.HIGH_PRIORITY}'")

        if PREFECT_CONSTANTS.QUEUES.DEFAULT not in [q.name for q in existing_queues]:
            await client.create_work_queue(
                name=PREFECT_CONSTANTS.QUEUES.DEFAULT,
                concurrency_limit=5,
                priority=10,
                work_pool_name=work_pool,
            )
            print(f"Created new work queue '{PREFECT_CONSTANTS.QUEUES.DEFAULT}'")

        if PREFECT_CONSTANTS.QUEUES.LOW not in [q.name for q in existing_queues]:
            await client.create_work_queue(
                name=PREFECT_CONSTANTS.QUEUES.LOW,
                concurrency_limit=1,
                priority=30,
                work_pool_name=work_pool,
            )
            print(f"Created new work queue '{PREFECT_CONSTANTS.QUEUES.LOW}'")

    @staticmethod
    async def _create_variables(client):
        default_variables = {}

        for var_name, var_value in default_variables.items():
            current = await client.read_variable_by_name(var_name)
            if current is None:
                result = await client.create_variable(
                    prefect.client.schemas.actions.VariableCreate(
                        name=var_name,
                        value=var_value,
                        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
                    )
                )
                print(f"Created new variable '{result}'")
            else:
                print(f"Variable '{var_name}' already exists")

    @staticmethod
    async def _create_blocks(client):
        default_blocks = [
            (
                PREFECT_CONSTANTS.POLL_HK.WEBPODA_AUTH_CODE_SECRET_NAME,
                prefect.blocks.system.Secret(value=SecretStr("")),
            ),
            (
                PREFECT_CONSTANTS.POLL_SCIENCE.SDC_AUTH_CODE_SECRET_NAME,
                prefect.blocks.system.Secret(value=SecretStr("")),
            ),
        ]
        blocks = await client.read_block_documents()
        for block_name, block in default_blocks:
            if block_name not in [b.name for b in blocks]:
                await block.save(block_name, client=client)
                print(f"Created new block '{block_name}'")
            else:
                print(f"Block '{block_name}' already exists")
