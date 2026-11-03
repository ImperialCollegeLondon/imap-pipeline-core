from pydantic import BaseModel


class ColumnCheckConfig(BaseModel):
    """Configuration for checking a specific column in a file."""

    column_name: str
    check_for_empty: bool = True
    check_for_bad_data: bool = True
    match_as_bad_data: list[str] = ["-9.999999848243207e+30", "NA", "N/A", "-"]
    expected_range: list[float] | None = None  # [min, max]


class FileAnalysisPatternConfig(BaseModel):
    """Configuration for a specific file pattern to index."""

    pattern: str  # fnmatch glob pattern for matching files
    datetime_column: str | None = None  # explicit datetime column name
    expected_time_between_records: str | None = None  # e.g. "00:00:04" (HH:MM:SS)
    cdf_attributes_to_index: list[str] = []
    columns_to_check: list[ColumnCheckConfig] = []


class FileAnalysisConfig(BaseModel):
    """Configuration for the file index flow."""

    paths_to_match: list[str] = []  # fnmatch patterns for finding files to index
    file_patterns: list[FileAnalysisPatternConfig] = []  # per-file pattern configs
    default_gap_threshold_seconds: int = 60  # 60 seconds = 1 minute
    # A sentinel is a special value stored in place of real data to mean "no valid
    # measurement here" (NaN itself cannot be used as it is awkward to store and
    # compare in CDF files). Any value equal to the sentinel is bad data, not a
    # real reading, so it must be excluded from stats and counted as bad.
    # -1e31 (the CDF float fill value) as stored in float32, hence the odd digits.
    # Each CDF variable's own FILLVAL attribute is also treated as bad data, which
    # covers non-float types such as TT2000 epochs.
    nan_sentinel: float = -9.999999848243207e30
    # Maximum number of files to analyse in one automatic (scheduled) run, so a
    # large backlog is worked through over several runs. None means no limit.
    max_files_per_run: int | None = None
