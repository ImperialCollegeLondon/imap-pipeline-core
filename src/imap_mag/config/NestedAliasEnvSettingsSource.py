from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel
from pydantic._internal._utils import lenient_issubclass
from pydantic.fields import FieldInfo
from pydantic_settings import EnvSettingsSource


class NestedAliasEnvSettingsSource(EnvSettingsSource):
    """
    Custom settings source that loads nested alias environment variables.

    Pydantic no longer loads nested alias environment variables, so we need to
    do it ourselves.
    This class reuses most of the logic from `EnvSettingsSource` but overrides
    the `explode_env_vars` method to also retrieve nested aliases.
    """

    def explode_env_vars(
        self, field_name: str, field: FieldInfo, env_vars: Mapping[str, str | None]
    ) -> dict[str, Any]:
        # Find original env vars
        parent_results: dict[str, Any] = super().explode_env_vars(
            field_name, field, env_vars
        )

        # Add nested env vars by looking for aliases (without any prefixes)
        alias_results: dict[str, Any] = self.__get_alias_values(
            field_name, field, env_vars
        )
        alias_results = alias_results[field_name] if alias_results else {}

        # Merge results by prioritizing parent results
        return self.__merge_results(parent_results, alias_results)

    def __get_alias_values(
        self, field_name: str, field: FieldInfo, env_vars: Mapping[str, str | None]
    ) -> dict[str, Any]:
        if field.annotation and lenient_issubclass(field.annotation, BaseModel):
            results: dict[str, Any] = {}
            for subfield_name, subfield in field.annotation.model_fields.items():
                results.update(
                    self.__get_alias_values(subfield_name, subfield, env_vars)
                )
            return {field_name: results}

        elif field.validation_alias is not None:
            field_info: list[tuple[str, str, bool]] = self._extract_field_info(
                field, field_name
            )
            for _, env_var, _ in field_info:
                if env_var in env_vars:
                    return {field_name: env_vars[env_var]}

        return {}

    def __merge_results(
        self, parent_results: dict[str, Any], alias_results: dict[str, Any]
    ) -> dict[str, Any]:
        merged_results: dict[str, Any] = parent_results.copy()

        for key, value in alias_results.items():
            if key in merged_results:
                if isinstance(merged_results[key], dict):
                    assert isinstance(value, dict)  # make sure value is also a dict
                    merged_results[key] = self.__merge_results(
                        merged_results[key], value
                    )
                elif not merged_results[key]:
                    merged_results[key] = value  # discard any empty/None value
                else:
                    print(
                        f"Conflicting values for '{key}': {merged_results[key]} (original) and {value} (alias). Discarding alias value."
                    )
            else:
                merged_results[key] = value

        return merged_results
