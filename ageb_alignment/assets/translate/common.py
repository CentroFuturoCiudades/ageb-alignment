import numpy as np
import pandas as pd

from pathlib import Path
from typing import Callable


def generate_options_str(gcp: np.ndarray) -> str:
    options_str = "-tps -t_srs EPSG:6372 "
    for row in gcp:
        options_str += "-gcp " + " ".join(row.astype(str)) + " "
    return options_str


def load_final_gcp_factory(year: int) -> Callable:
    def _op(manual_path: Path, partition_key: str) -> np.ndarray:
        gcp_path = manual_path / f"gcp/{year}/{partition_key}.points"
        points = pd.read_csv(
            gcp_path, usecols=["sourceX", "sourceY", "mapX", "mapY"], header=1
        )
        points = points[
            ["sourceX", "sourceY", "mapX", "mapY"]
        ].to_numpy()  # Ensure right order
        return points

    return _op


load_final_gcp_1990 = load_final_gcp_factory(1990)
load_final_gcp_2000 = load_final_gcp_factory(2000)
