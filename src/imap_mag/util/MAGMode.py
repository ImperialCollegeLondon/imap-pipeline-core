from enum import Enum


class MAGMode(int, Enum):
    """MAG operation modes."""

    Standby = 1
    Safe = 2
    Config = 3
    Debug = 4
    Normal = 5
    Burst = 6
