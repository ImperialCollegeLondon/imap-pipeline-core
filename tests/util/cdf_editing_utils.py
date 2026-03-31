"""CLI utility for editing CDF files. Useful for making test data!

Usage:
    python tests/util/cdf_editing_utils.py set-date <input_cdf> <date> <output_cdf>

Commands:
    set-date    Shift all epoch variables in a CDF file so they occur on the given date,
                preserving time-of-day for each record.
"""

from pathlib import Path

import cdflib
import numpy as np
import typer

app = typer.Typer(no_args_is_help=True)


@app.callback()
def callback() -> None:
    """CDF file editing utilities."""


# CDF data type codes for epoch/time variables
_EPOCH_DATA_TYPES = {
    31,  # CDF_EPOCH
    32,  # CDF_EPOCH16
    33,  # CDF_TIME_TT2000
}


def _shift_epoch_tt2000(epoch: np.ndarray, target_date: np.datetime64) -> np.ndarray:
    """Shift TT2000 epoch values so all records fall on target_date, preserving time-of-day."""
    dt_arr = cdflib.cdfepoch.to_datetime(epoch)
    first_date = np.datetime64(str(dt_arr[0])[:10], "D")
    delta_ns = int((target_date - first_date) / np.timedelta64(1, "ns"))
    return epoch + np.int64(delta_ns)


def _shift_epoch_epoch(epoch: np.ndarray, target_date: np.datetime64) -> np.ndarray:
    """Shift CDF_EPOCH (milliseconds since 0 AD) values, preserving time-of-day."""
    dt_arr = cdflib.cdfepoch.to_datetime(epoch)
    first_date = np.datetime64(str(dt_arr[0])[:10], "D")
    delta_ms = int((target_date - first_date) / np.timedelta64(1, "ms"))
    return epoch + np.float64(delta_ms)


def _build_var_spec(var_info) -> dict:
    """Build a var_spec dict for cdflib write_var from a VDRInfo object."""
    spec = {
        "Variable": var_info.Variable,
        "Data_Type": var_info.Data_Type,
        "Num_Elements": var_info.Num_Elements,
        "Rec_Vary": var_info.Rec_Vary,
        "Var_Type": var_info.Var_Type,
        "Sparse": var_info.Sparse,
        "Compress": var_info.Compress,
        "Block_Factor": var_info.Block_Factor,
    }
    if var_info.Var_Type == "zVariable":
        spec["Dim_Sizes"] = var_info.Dim_Sizes
    else:
        spec["Dim_Vary"] = var_info.Dim_Vary
    if var_info.Pad is not None:
        spec["Pad"] = var_info.Pad
    return spec


def _convert_var_attrs(attrs: dict) -> dict:
    """Convert varattsget output to a format suitable for write_var var_attrs."""
    result = {}
    for key, value in attrs.items():
        if isinstance(value, np.integer):
            result[key] = int(value)
        elif isinstance(value, np.floating):
            result[key] = float(value)
        elif isinstance(value, np.ndarray):
            result[key] = value.tolist()
        else:
            result[key] = value
    return result


def _convert_global_attrs(attrs: dict) -> dict:
    """Convert globalattsget output to the entry-number-keyed format for write_globalattrs."""
    result = {}
    for key, value in attrs.items():
        if isinstance(value, list):
            result[key] = {i: v for i, v in enumerate(value)}
        else:
            result[key] = {0: value}
    return result


@app.command()
def set_date(
    input_file: Path = typer.Argument(..., help="Path to the source CDF file"),
    date: str = typer.Argument(
        ..., help="Target date in YYYYMMDD or YYYY-MM-DD format"
    ),
    output_file: Path = typer.Argument(..., help="Path for the output CDF file"),
) -> None:
    """Shift all epoch variables in a CDF file so they occur on the given date.

    Time-of-day is preserved for each record. The date of the first epoch record
    is used as the reference; all epochs are shifted by the same delta.
    """
    # Parse target date
    date_str = date.replace("-", "")
    if len(date_str) != 8:
        typer.echo(
            f"Error: date must be in YYYYMMDD or YYYY-MM-DD format, got: {date}",
            err=True,
        )
        raise typer.Exit(1)
    target_date = np.datetime64(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}", "D")

    src = cdflib.CDF(input_file)
    info = src.cdf_info()

    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Remove .cdf suffix for cdfwrite (it appends it automatically)
    out_path = str(output_file)
    if out_path.lower().endswith(".cdf"):
        out_path = out_path[:-4]

    cdf_spec = {
        "Majority": info.Majority,
        "Encoding": info.Encoding,
        "Checksum": info.Checksum,
        "Compressed": info.Compressed,
    }

    all_vars = info.rVariables + info.zVariables
    modified_count = 0

    with cdflib.cdfwrite.CDF(out_path, cdf_spec=cdf_spec, delete=True) as dst:
        # Write global attributes
        global_attrs = _convert_global_attrs(src.globalattsget())
        dst.write_globalattrs(global_attrs)

        # Write each variable
        for var_name in all_vars:
            var_info = src.varinq(var_name)
            var_spec = _build_var_spec(var_info)
            var_attrs = _convert_var_attrs(src.varattsget(var_name))

            data = src.varget(var_name)

            # Shift epoch variables
            if var_info.Data_Type == 33:  # CDF_TIME_TT2000
                data = _shift_epoch_tt2000(data, target_date)
                modified_count += 1
            elif var_info.Data_Type == 31:  # CDF_EPOCH
                data = _shift_epoch_epoch(data, target_date)
                modified_count += 1

            dst.write_var(var_spec, var_attrs=var_attrs, var_data=data)

    typer.echo(
        f"Written {output_file} with {modified_count} epoch variable(s) shifted to {target_date}"
    )


if __name__ == "__main__":
    app()
