from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from ageb_alignment.defs.resources import PathResource


@dg.asset(
    key=["geometry", "blocks", "2020"],
    io_manager_key="gpkg_manager",
    group_name="geometry_blocks",
)
def geometry_blocks_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    states_path = Path(path_resource.data_path) / "initial" / "geometry" / "states"

    df = []
    for dir_path in states_path.glob("*"):
        if not dir_path.is_dir():
            continue

        prefix = dir_path.stem.split("_")[0]
        temp = (
            gpd.read_file(dir_path / f"{prefix}m.shp")
            .query("(AMBITO == 'Urbana') & (TIPOMZA == 'Típica')")
            .drop(columns=["AMBITO", "TIPOMZA"])
        )
        df.append(temp)

    df = pd.concat(df).to_crs("EPSG:6372")
    return df
