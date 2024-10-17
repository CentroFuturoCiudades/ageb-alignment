import numpy as np
import pandas as pd

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource
from dagster import asset, AssetDep, AssetExecutionContext
from pathlib import Path


def gcp_final_factory(year: int) -> asset:
    @asset(
        name=str(year),
        key_prefix=["gcp", "final"],
        deps=[AssetDep(["gcp", "initial", str(year)])],
        partitions_def=zone_partitions,
    )
    def _asset(
        context: AssetExecutionContext, path_resource: PathResource
    ) -> np.ndarray:
        gcp_path = (
            Path(path_resource.intermediate_path)
            / f"gcp/{year}/{context.partition_key}.points"
        )
        points = pd.read_csv(
            gcp_path, usecols=["sourceX", "sourceY", "mapX", "mapY"], header=1
        )
        points = points[
            ["sourceX", "sourceY", "mapX", "mapY"]
        ].to_numpy()  # Ensure right order
        return points

    return _asset


gcp_final_assets = [gcp_final_factory(year) for year in (1990, 2000)]
