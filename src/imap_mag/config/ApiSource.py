from pydantic import BaseModel, ConfigDict, Field, SecretStr

from imap_mag.util.constants import CONSTANTS


class ApiSource(BaseModel):
    url_base: str
    auth_code: SecretStr | None = None

    model_config = ConfigDict(populate_by_name=True)


class IALiRTApiSource(ApiSource):
    url_base: str = Field(validation_alias=CONSTANTS.ENV_VAR_NAMES.IALIRT_URL)
    auth_code: SecretStr | None = Field(
        validation_alias=CONSTANTS.ENV_VAR_NAMES.IALIRT_AUTH_CODE, default=None
    )


class WebPodaApiSource(ApiSource):
    auth_code: SecretStr | None = Field(
        validation_alias=CONSTANTS.ENV_VAR_NAMES.WEBPODA_AUTH_CODE, default=None
    )


class WebTCADLaTiSApiSource(ApiSource):
    auth_code: SecretStr | None = Field(
        validation_alias=CONSTANTS.ENV_VAR_NAMES.WEBPODA_AUTH_CODE, default=None
    )
    system_id: str  # "SID1" or "SID2", to distinguish between flight and preflight data


class SdcApiSource(ApiSource):
    url_base: str = Field(validation_alias=CONSTANTS.ENV_VAR_NAMES.SDC_URL)
    auth_code: SecretStr | None = Field(
        validation_alias=CONSTANTS.ENV_VAR_NAMES.SDC_AUTH_CODE, default=None
    )
