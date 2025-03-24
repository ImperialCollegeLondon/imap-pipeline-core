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


async def get_secret_block(secret_name: str) -> str:
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


async def get_secret_or_env_var(secret_name: str, env_var_name: str) -> str:
    logger = get_run_logger()

    auth_code: str | None = None

    try:
        auth_code = await get_secret_block(secret_name)
    except ValueError:
        logger.info(
            f"{secret_name} not found or empty. Using environment variable {env_var_name}."
        )

    if not auth_code:
        auth_code = os.getenv(env_var_name)

    if not auth_code:
        logger.error(
            f"Environment variable {env_var_name} and secret {secret_name} are both undefined."
        )
        raise ValueError(
            f"Environment variable {env_var_name} and secret {secret_name} are both undefined."
        )

    return auth_code
