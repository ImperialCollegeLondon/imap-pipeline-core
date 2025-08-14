import logging
from pathlib import Path

import matplotlib.pyplot as plt

from mag_toolkit.calibration import ScienceLayer
from mag_toolkit.calibration.MatlabWrapper import call_matlab

logger = logging.getLogger(__name__)


class Plotter:
    """
    A class to handle plotting operations for the IMAP MAG toolkit.
    """

    def __init__(self, work_folder: Path):
        self.work_folder = work_folder

    def plot(self, layer: ScienceLayer, filename: str = ""):
        """
        Plot the given data and save it to a file.

        :param data: Data to plot.
        :param title: Title of the plot.
        :param filename: Name of the file to save the plot.
        """

        science_root = Path(layer.science_file)

        if not filename:
            filename = f"plot_{science_root.name}.png"

        data = layer.as_df()
        if data.empty:
            raise ValueError("No data available to plot.")

        plt.figure()
        plt.plot(data["epoch"], data["x"], label="Value")
        plot_title = f"Plot of {science_root.name}"
        plt.title(plot_title)
        plt.savefig(self.work_folder / filename)
        plt.close()

        return self.work_folder / filename

    def matlab_plot(source_file: Path, output_file: Path):
        call_matlab(
            f'calibration.wrappers.plot_data("{source_file!s}", "{output_file!s}")'
        )
