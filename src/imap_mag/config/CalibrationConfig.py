from pathlib import Path

import yaml
from pydantic import BaseModel


class GradiometryConfig(BaseModel):
    kappa: float = 0.0
    sc_interference_threshold: float = 0.0


class CalibrationConfig(BaseModel):
    gradiometer: GradiometryConfig = GradiometryConfig()

    @classmethod
    def from_file(cls, path: Path):
        with open(path) as fid:
            as_dict = yaml.safe_load(fid)
        model = cls(**as_dict)
        return model
