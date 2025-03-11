import prefect
import prefect.docker
from prefect import get_client
from pydantic import SecretStr

from imap_db.main import create_db, upgrade_db
from prefect_server.constants import CONSTANTS


class ServerConfig:
    @staticmethod
    async def initialise():
        # Create IMAP database
        create_db()
        upgrade_db()

        # Initialize server configuration
        async with get_client() as client:
            await ServerConfig._create_concurrency_limits(client)
            await ServerConfig._create_queues(client)
            await ServerConfig._create_variables(client)
            await ServerConfig._create_blocks(client)

    @staticmethod
    async def _create_concurrency_limits(client):
        pass

    @staticmethod
    async def _create_queues(client):
        existing_queues = await client.read_work_queues()
        if CONSTANTS.QUEUES.HIGH_PRIORITY not in [q.name for q in existing_queues]:
            await client.create_work_queue(
                name=CONSTANTS.QUEUES.HIGH_PRIORITY,
                concurrency_limit=1,
                priority=1,
                work_pool_name=CONSTANTS.DEFAULT_WORKPOOL,
            )
            print(f"Created new work queue '{CONSTANTS.QUEUES.HIGH_PRIORITY}'")

        if CONSTANTS.QUEUES.DEFAULT not in [q.name for q in existing_queues]:
            await client.create_work_queue(
                name=CONSTANTS.QUEUES.DEFAULT,
                concurrency_limit=5,
                priority=10,
                work_pool_name=CONSTANTS.DEFAULT_WORKPOOL,
            )
            print(f"Created new work queue '{CONSTANTS.QUEUES.DEFAULT}'")

        if CONSTANTS.QUEUES.LOW not in [q.name for q in existing_queues]:
            await client.create_work_queue(
                name=CONSTANTS.QUEUES.LOW,
                concurrency_limit=1,
                priority=30,
                work_pool_name=CONSTANTS.DEFAULT_WORKPOOL,
            )
            print(f"Created new work queue '{CONSTANTS.QUEUES.LOW}'")

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
                        tags=[CONSTANTS.PREFECT_TAG],
                    )
                )
                print(f"Created new variable '{result}'")
            else:
                print(f"Variable '{var_name}' already exists")

    @staticmethod
    async def _create_blocks(client):
        default_blocks = [
            (
                CONSTANTS.POLL_HK.WEBPODA_AUTH_CODE_SECRET_NAME,
                prefect.blocks.system.Secret(value=SecretStr("some-webpoda-auth-code")),
            ),
        ]
        blocks = await client.read_block_documents()
        for block_name, block in default_blocks:
            if block_name not in [b.name for b in blocks]:
                await block.save(block_name, client=client)
                print(f"Created new block '{block_name}'")
            else:
                print(f"Block '{block_name}' already exists")
