import typing
from pathlib import Path, PurePosixPath

import pytest
import typing_extensions
from wiremock.client import (
    HttpMethods,
    Mapping,
    MappingRequest,
    MappingResponse,
    Mappings,
)
from wiremock.constants import Config
from wiremock.testing.testcontainer import WireMockContainer, wiremock_container


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

    def __del__(self):
        Mappings.delete_all_mappings()

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

        container_dir_path = PurePosixPath("/home/wiremock/__files")

        with open(host_path, "rb") as f:
            self.__mock_container.copy_files_to_container(
                {host_path: f.read()}, container_dir_path, "wb"
            )

        self.__add_mapping(url, Path(host_path).name, is_file=True, **options)

    def add_mapping(mapping: Mapping) -> None:
        """Add WireMock mapping for URL."""

        Mappings.create_mapping(mapping)

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


@pytest.fixture(scope="session", autouse=False)
def wiremock_manager():
    with wiremock_container(secure=False) as mock_container:
        yield WireMockManager(mock_container)
