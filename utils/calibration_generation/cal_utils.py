import logging
from pathlib import Path

import numpy as np
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


def get_one_sensor_stacked_offsets(x_offset, y_offset, z_offset):
    offsets = np.array([x_offset, y_offset, z_offset])
    one_sensor = np.stack([offsets, offsets, offsets, offsets], axis=0)
    return one_sensor


def get_correctly_arranged_offsets(mago_offsets, magi_offsets):
    if mago_offsets.shape != (4, 3) or magi_offsets.shape != (4, 3):
        raise ValueError("Offsets must be of shape (4, 3)")
    return np.stack([mago_offsets, magi_offsets], axis=0)


def get_default_matrices():
    mago_default = [
        [0.999989918615689, -3.25282523386058e-05, -0.00477484817389527],
        [-0.00161934234084477, 1.00081282729992, -0.0107947265795152],
        [0.00449876643714237, 0.0068123135754105, 0.999984957597784],
    ]
    magi_default = [
        [1.0011485845956, -0.00335794534947757, -0.00337416446398934],
        [0.000583975986099036, 1.00042889375192, -0.00962198497343362],
        [0.0050384296101901, 0.00443047384468987, 0.999976121125062],
    ]
    mago_stack = np.stack(
        [mago_default, mago_default, mago_default, mago_default], axis=2
    )
    magi_stack = np.stack(
        [magi_default, magi_default, magi_default, magi_default], axis=2
    )
    return mago_stack, magi_stack


def get_stacked_identity_matrices():
    i = np.eye(3, 3)
    return np.stack([i, i, i, i], axis=2)


def verify_frame_transforms(cdf: pycdf.CDF):
    mago_transform = cdf["URFTOORFO"][:]
    magi_transform = cdf["URFTOORFI"][:]
    if mago_transform is None or magi_transform is None:
        raise ValueError("Frame transform data is empty in the CDF file.")
    assert mago_transform.shape == (
        3,
        3,
        4,
    ), "MAGo frame transform shape is incorrect, expected (3, 3, 4)"
    assert magi_transform.shape == (
        3,
        3,
        4,
    ), "MAGi frame transform shape is incorrect, expected (3, 3, 4)"
    range_3_mago = mago_transform[:, :, 3]
    range_3_magi = magi_transform[:, :, 3]
    print("Manually Verify Range 3 MAGo Frame Transform Matrix:")
    print(range_3_mago)
    print("Manually Verify Range 3 MAGi Frame Transform Matrix:")
    print(range_3_magi)
    return True


def verify_offsets(cdf: pycdf.CDF):
    offsets = cdf["offsets"][:]
    if offsets is None:
        logging.error("Offsets data is empty in the CDF file.")
        return False

    if offsets.shape != (2, 4, 3):
        logging.error(
            f"Offsets data shape is incorrect, expected (2, 4, 3), got {offsets.shape}"
        )
        return False

    mago_offsets = offsets[0, :, :]
    magi_offsets = offsets[1, :, :]
    offsets_valid = True
    for r in range(4):
        magnitude_mago_offsets = np.linalg.norm(mago_offsets[r, :])
        magnitude_magi_offsets = np.linalg.norm(magi_offsets[r, :])
        if magnitude_mago_offsets >= magnitude_magi_offsets:
            offsets_valid = False
            logging.error(
                f"MAGo offsets magnitude {magnitude_mago_offsets} is greater than or equal to MAGi offsets magnitude {magnitude_magi_offsets} for range {r}"
            )
    return offsets_valid


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


def generate_l2_file(
    version,
    valid_start_date,
    frame_transform_mago,
    frame_transform_magi,
):
    filename = f"imap_mag_l2-calibration_{valid_start_date.strftime('%Y%m%d')}_v{version:03d}.cdf"
    if Path(filename).exists():
        raise FileExistsError(
            f"File {filename} already exists. Please choose a different version or date."
        )
    cdf = pycdf.CDF(
        filename,
        "",
    )
    cdf_manager = get_cdf_attribute_manager()

    set_default_cdf_attrs(cdf, cdf_manager, "imap_mag_l2-calibration")
    setup_variable(cdf, cdf_manager, "range", [0, 1, 2, 3])
    setup_variable(cdf, cdf_manager, "URFTOORFO", frame_transform_mago)
    setup_variable(cdf, cdf_manager, "URFTOORFI", frame_transform_magi)
    cdf.close()
    return filename


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


def set_default_cdf_attrs(cdf: pycdf.CDF, cdf_manager: CdfAttributeManager, source_id):
    global_attrs = cdf_manager.get_global_attributes(source_id)
    for key in global_attrs:
        if global_attrs[key] is not None:
            cdf.attrs[key] = global_attrs[key]
    return cdf
