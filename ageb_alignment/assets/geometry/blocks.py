import dagster as dg
import geopandas as gpd
import pandas as pd

from ageb_alignment.resources import PathResource
from pathlib import Path


@dg.asset(name="2020", key_prefix=["geometry", "blocks"], io_manager_key="gpkg_manager")
def geometry_blocks_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    states_path = Path(path_resource.raw_path) / "geometry/states"

    df = []
    for dir_path in states_path.glob("*"):
        if not dir_path.is_dir():
            continue
        
        prefix = dir_path.stem.split("_")[0]
        temp = gpd.read_file(dir_path / f"{prefix}m.shp").query("(AMBITO == 'Urbana') & (TIPOMZA == 'Típica')").drop(columns=["AMBITO", "TIPOMZA"])
        df.append(temp)

    df = pd.concat(df).to_crs("EPSG:6372")
    return df