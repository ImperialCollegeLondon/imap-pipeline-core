"""App configuration module."""

import os
import tempfile
from pathlib import Path, PosixPath, WindowsPath
from typing import Optional

import yaml
from pydantic import BaseModel
from pydantic.aliases import AliasGenerator
from pydantic.config import ConfigDict


def hyphenize(field: str):
    return field.replace("_", "-")


class Source(BaseModel):
    folder: Path


class Destination(BaseModel):
    folder: Path = Path(".")
    filename: str
    export_to_database: bool = True


class PacketDefinition(BaseModel):
    hk: Path


class API(BaseModel):
    webpoda_url: Optional[str] = None
    sdc_url: Optional[str] = None


class AppConfig(BaseModel):
    source: Source
    work_folder: Path = Path(".work")
    destination: Destination
    packet_definition: Optional[PacketDefinition] = None
    api: Optional[API] = None

    def __init__(self, **kwargs):
        # Replace hypens with underscores so that you can build config from constructor args,
        # and still have them mapped to the hyphen split property names in the YAML files.
        kwargs = dict((key.replace("_", "-"), value) for (key, value) in kwargs.items())
        super().__init__(**kwargs)

    # pydantic configuration to allow hyphenated fields
    model_config = ConfigDict(
        alias_generator=AliasGenerator(
            validation_alias=hyphenize, serialization_alias=hyphenize
        )
    )


def create_serialize_config(
    *,
    source: Path = Path("."),
    destination_folder: Path = Path("output"),
    destination_file: str = "results.csv",
    webpoda_url: str | None = None,
    sdc_url: str | None = None,
    export_to_database: bool = True,
) -> tuple[AppConfig, Path]:
    """Create and serialize a configuration object."""

    config = AppConfig(
        source=Source(folder=source),
        destination=Destination(
            folder=destination_folder,
            filename=destination_file,
            export_to_database=export_to_database,
        ),
        packet_definition=PacketDefinition(hk=Path("xtce/tlm_20241024.xml")),
        api=API(webpoda_url=webpoda_url, sdc_url=sdc_url),
    )

    if not os.path.exists(config.work_folder):
        os.makedirs(config.work_folder)

    config_file = Path(tempfile.mkdtemp()) / "config-temp.yaml"

    with open(config_file, "w") as f:
        for path_type in (PosixPath, WindowsPath):
            yaml.add_representer(
                path_type, lambda dumper, data: dumper.represent_str(str(data))
            )
        yaml.dump(config.model_dump(by_alias=True), f)

    return (config, config_file)
