import typing
from pathlib import Path

import typing_extensions
from wiremock.client import (
    HttpMethods,
    Mapping,
    MappingRequest,
    MappingResponse,
    Mappings,
)
from wiremock.constants import Config
from wiremock.testing.testcontainer import WireMockContainer


class MappingOptions(typing.TypedDict):
    """Options for mapping."""

    is_pattern: bool
    status: int
    priority: int | None


class WireMockManager:
    """Manage mocking of URL."""

    __mock_container: WireMockContainer

    def __init__(self, mock_container: WireMockContainer):
        self.__mock_container = mock_container
        Config.base_url = self.__mock_container.get_url("__admin")

    def get_url(self) -> str:
        return self.__mock_container.get_url("/")

    def add_string_mapping(
        self,
        url: str,
        body: str,
        **options: typing_extensions.Unpack[MappingOptions],
    ) -> None:
        """Add WireMock string mapping for URL."""

        self.__add_mapping(url, body, is_file=False, **options)

    def add_file_mapping(
        self,
        url: str,
        host_path: str,
        **options: typing_extensions.Unpack[MappingOptions],
    ) -> None:
        """Copy file to container and add WireMock mapping for it."""

        container_dir_path = Path(self.__mock_container.FILES_DIR)

        with open(host_path, "rb") as f:
            self.__mock_container.copy_files_to_container(
                {host_path: f.read()}, container_dir_path, "wb"
            )

        self.__add_mapping(url, Path(host_path).name, is_file=True, **options)

    def add_mapping(self, mapping: Mapping) -> None:
        """Add WireMock mapping for URL."""

        Mappings.create_mapping(mapping)

    def reset(self) -> None:
        """Reset WireMock server."""

        Mappings.delete_all_mappings()

    def __add_mapping(
        self,
        url: str,
        body: str,
        *,
        is_file: bool,
        **options: typing_extensions.Unpack[MappingOptions],
    ) -> None:
        request = MappingRequest(
            method=HttpMethods.GET,
        )

        if options["is_pattern"] if "is_pattern" in options else False:
            request.url_pattern = url
        else:
            request.url = url

        response = MappingResponse(
            status=options["status"] if "status" in options else 200
        )

        if is_file:
            response.body_file_name = body
        else:
            response.body = body

        mapping = Mapping(
            request=request,
            response=response,
            persistent=False,
        )

        if (options["priority"] if "priority" in options else None) is not None:
            mapping.priority = options["priority"]

        Mappings.create_mapping(mapping)

        # clone and add for /api-key prefix
        api_key_mapping = Mapping.from_json(mapping.to_json())
        if api_key_mapping.request.url is not None:
            api_key_mapping.request.url = "/api-key" + api_key_mapping.request.url
        if api_key_mapping.request.url_pattern is not None:
            api_key_mapping.request.url_pattern = (
                "/api-key" + api_key_mapping.request.url_pattern
            )
        Mappings.create_mapping(api_key_mapping)
