import tempfile

import geopandas as gpd
import numpy as np
import pandas as pd

from osgeo import gdal
from pathlib import Path

gdal.UseExceptions()


def generate_options_str(gcp: np.ndarray) -> str:
    options_str = "-tps -t_srs EPSG:6372 "
    for row in gcp:
        options_str += "-gcp " + " ".join(row.astype(str)) + " "
    return options_str


def load_gcp(gcp_path: Path) -> np.ndarray:
    points = pd.read_csv(
        gcp_path, usecols=["sourceX", "sourceY", "mapX", "mapY"], header=1
    )
    points = points[
        ["sourceX", "sourceY", "mapX", "mapY"]
    ].to_numpy()  # Ensure right order
    return points


def translate_geometries_single(ageb_path: Path, options: str) -> gpd.GeoDataFrame:
    with tempfile.TemporaryDirectory() as temp_dir:
        out_path = Path(temp_dir) / "df.gpkg"
        gdal.VectorTranslate(str(out_path), str(ageb_path), options=options)
        gdf = gpd.read_file(out_path)
    return gdf


def translate_geometries_double(
    ageb_path: Path, options_first: str, options_second: str
) -> gpd.GeoDataFrame:

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        temp_path = temp_dir / "df_temp.gpkg"
        out_path = temp_dir / "df.gpkg"

        gdal.VectorTranslate(str(temp_path), str(ageb_path), options=options_first)
        gdal.VectorTranslate(str(out_path), str(temp_path), options=options_second)

        gdf = gpd.read_file(out_path)
    return gdf
