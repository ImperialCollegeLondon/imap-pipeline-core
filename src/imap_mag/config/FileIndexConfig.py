from pydantic import BaseModel


class ColumnCheckConfig(BaseModel):
    """Configuration for checking a specific column in a file."""

    column_name: str
    check_for_empty: bool = True
    check_for_bad_data: bool = True
    match_as_bad_data: list[str] = ["-9.999999848243207e+30", "NA", "N/A", "-"]
    expected_range: list[float] | None = None  # [min, max]


class FileIndexPatternConfig(BaseModel):
    """Configuration for a specific file pattern to index."""

    pattern: str  # fnmatch glob pattern for matching files
    datetime_column: str | None = None  # explicit datetime column name
    expected_time_between_records: str | None = None  # e.g. "00:00:04" (HH:MM:SS)
    cdf_attributes_to_index: list[str] = []
    columns_to_check: list[ColumnCheckConfig] = []


class FileIndexConfig(BaseModel):
    """Configuration for the file index flow."""

    paths_to_match: list[str] = []  # fnmatch patterns for finding files to index
    file_patterns: list[FileIndexPatternConfig] = []  # per-file pattern configs
    default_gap_threshold_seconds: int = 60  # 60 seconds = 1 minute
    nan_sentinel: float = -9.999999848243207e30
