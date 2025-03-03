import os

from prefect import get_run_logger
from prefect.blocks.system import Secret


def get_cron_from_env(env_var_name: str, default: str | None = None) -> str | None:
    cron = os.getenv(env_var_name, default)

    if cron is None or cron == "":
        return None
    else:
        cron = cron.strip(" '\"")
        print(f"Using cron schedule: {env_var_name}={cron}")
        return cron


# TODO: This is copied from so-pipeline-core
async def get_secret_block(secret_name: str):
    logger = get_run_logger()

    logger.info(f"Retrieving secret block {secret_name}.")

    try:
        secret: Secret = await Secret.aload(secret_name)
    except ValueError as e:
        logger.error(f"Block {secret_name} does not exist.")
        raise e

    value = secret.get()

    if not value:
        logger.error(f"Block {secret_name} is empty.")
        raise ValueError(f"Block {secret_name} is empty.")

    logger.debug(f"Block {secret_name} retrieved successfully.")

    return value
