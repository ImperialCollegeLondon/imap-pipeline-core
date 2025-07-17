from pathlib import Path

from sammi.cdf_attribute_manager import CdfAttributeManager
from spacepy import pycdf

global_layers = [
    Path("utils/calibration_generation/cdf/imap_default_global_cdf_attrs.yaml"),
    Path("utils/calibration_generation/cdf/imap_mag_global_cdf_attrs.yaml"),
]
variable_layers = [
    Path("utils/calibration_generation/cdf/imap_mag_calibration_cdf_attrs.yaml"),
    Path("utils/calibration_generation/cdf/imap_constant_attrs.yaml"),
]


def get_cdf_attribute_manager():
    cdf_manager = CdfAttributeManager(
        use_defaults=True,
    )
    for gl in global_layers:
        cdf_manager.load_global_attributes(gl)

    for vl in variable_layers:
        cdf_manager.load_variable_attributes(vl)
    return cdf_manager


def setup_variable(
    cdf,
    cdf_manager: CdfAttributeManager,
    name,
    value,
    var_type=None,
    recVary=True,
):
    variable_attrs = cdf_manager.get_variable_attributes(name)
    cdf.new(
        name,
        value,
        recVary=recVary,
        type=var_type,
    )
    for key in variable_attrs:
        cdf[name].attrs[key] = variable_attrs[key]


def generate_ialirt_file(
    version,
    valid_start_date,
    offsets,
    frame_transform_mago,
    frame_transform_magi,
    gradiometer_value,
):
    filename = f"imap_mag_ialirt-calibration_{valid_start_date.strftime('%Y%m%d')}_v{version:03d}.cdf"
    if Path(filename).exists():
        raise FileExistsError(
            f"File {filename} already exists. Please choose a different version or date."
        )
    cdf = pycdf.CDF(
        filename,
        "",
    )

    cdf_manager = get_cdf_attribute_manager()

    set_default_cdf_attrs(cdf, cdf_manager, "imap_mag_ialirt-calibration")
    setup_variable(cdf, cdf_manager, "axis", ["x", "y", "z"])
    setup_variable(cdf, cdf_manager, "sensor", ["MAGo", "MAGi"])
    setup_variable(cdf, cdf_manager, "range", [0, 1, 2, 3])
    setup_variable(
        cdf,
        cdf_manager,
        "valid_start_datetime",
        valid_start_date,
        recVary=False,
        var_type=pycdf.const.CDF_TIME_TT2000,
    )
    setup_variable(
        cdf,
        cdf_manager,
        "gradiometer_factor",
        gradiometer_value,
        var_type=pycdf.const.CDF_DOUBLE,
        recVary=False,
    )
    setup_variable(
        cdf,
        cdf_manager,
        "offsets",
        offsets,
        recVary=True,
        var_type=pycdf.const.CDF_DOUBLE,
    )
    setup_variable(cdf, cdf_manager, "URFTOORFO", frame_transform_mago)
    setup_variable(cdf, cdf_manager, "URFTOORFI", frame_transform_magi)
    cdf.close()
    return filename


def generate_l1d_file(
    version,
    valid_start_date,
    offsets,
    frame_transform_mago,
    frame_transform_magi,
    gradiometer_value,
    spin_average_factor,
    spin_num_cycles,
    quality_flag_threshold,
):
    filename = f"imap_mag_l1d-calibration_{valid_start_date.strftime('%Y%m%d')}_v{version:03d}.cdf"
    if Path(filename).exists():
        raise FileExistsError(
            f"File {filename} already exists. Please choose a different version or date."
        )
    cdf = pycdf.CDF(
        filename,
        "",
    )

    cdf_manager = get_cdf_attribute_manager()

    set_default_cdf_attrs(cdf, cdf_manager, "imap_mag_l1d-calibration")
    setup_variable(cdf, cdf_manager, "axis", ["x", "y", "z"])
    setup_variable(cdf, cdf_manager, "sensor", ["MAGo", "MAGi"])
    setup_variable(cdf, cdf_manager, "range", [0, 1, 2, 3])
    setup_variable(
        cdf,
        cdf_manager,
        "valid_start_datetime",
        valid_start_date,
        recVary=False,
        var_type=pycdf.const.CDF_TIME_TT2000,
    )
    setup_variable(
        cdf,
        cdf_manager,
        "gradiometer_factor",
        gradiometer_value,
        var_type=pycdf.const.CDF_DOUBLE,
        recVary=False,
    )
    setup_variable(
        cdf,
        cdf_manager,
        "offsets",
        offsets,
        recVary=True,
        var_type=pycdf.const.CDF_DOUBLE,
    )
    setup_variable(
        cdf,
        cdf_manager,
        "spin_average_application_factor",
        spin_average_factor,
        recVary=False,
        var_type=pycdf.const.CDF_DOUBLE,
    )
    setup_variable(
        cdf,
        cdf_manager,
        "number_of_spins",
        spin_num_cycles,
        recVary=False,
        var_type=pycdf.const.CDF_UINT4,
    )
    setup_variable(
        cdf,
        cdf_manager,
        "quality_flag_threshold",
        quality_flag_threshold,
        recVary=False,
        var_type=pycdf.const.CDF_DOUBLE,
    )
    setup_variable(cdf, cdf_manager, "URFTOORFO", frame_transform_mago)
    setup_variable(cdf, cdf_manager, "URFTOORFI", frame_transform_magi)
    cdf.close()
    return filename


def set_default_cdf_attrs(cdf, cdf_manager: CdfAttributeManager, source_id):
    global_attrs = cdf_manager.get_global_attributes(source_id)
    for key in global_attrs:
        if global_attrs[key] is not None:
            cdf.attrs[key] = global_attrs[key]
    return cdf
