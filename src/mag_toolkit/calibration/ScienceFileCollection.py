from datetime import datetime


class ScienceFileCollection:
    """
    A collection of the file names of all releveant science files for a date to pass to a calibration function
    It can choose to use one or all of them
    """

    def __init__(self, date: datetime):
        self.sciencefiles = []

    def __iter__(self):
        return iter(self.sciencefiles)

    def __len__(self):
        return len(self.sciencefiles)

    def __getitem__(self, item):
        return self.sciencefiles[item]
