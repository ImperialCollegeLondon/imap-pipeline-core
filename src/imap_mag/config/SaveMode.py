from enum import Enum


class SaveMode(str, Enum):
    LocalOnly = "LocalOnly"
    LocalAndDatabase = "LocalAndDatabase"
