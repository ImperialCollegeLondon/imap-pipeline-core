from pydantic import BaseModel


class ApiSource(BaseModel):
    url_base: str
    auth_code: str | None = None
