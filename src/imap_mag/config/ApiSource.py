from pydantic import BaseModel, Field, SecretStr

from imap_mag.util import CONSTANTS


class ApiSource(BaseModel):
    url_base: str
    auth_code: SecretStr | None = None


class WebPodaApiSource(ApiSource):
    auth_code: SecretStr | None = Field(
        validation_alias=CONSTANTS.ENV_VAR_NAMES.WEBPODA_AUTH_CODE, default=None
    )


class SdcApiSource(ApiSource):
    url_base: str = Field(
        validation_alias=CONSTANTS.ENV_VAR_NAMES.SDC_URL,
    )
    auth_code: SecretStr | None = Field(
        validation_alias=CONSTANTS.ENV_VAR_NAMES.SDC_AUTH_CODE, default=None
    )
