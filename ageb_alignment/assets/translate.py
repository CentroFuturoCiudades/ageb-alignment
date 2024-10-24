import os
import tempfile

import geopandas as gpd
import numpy as np
import pandas as pd

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource
from dagster import graph_asset, op, AssetIn, In, OpDefinition, OpExecutionContext, Out
from osgeo import gdal
from pathlib import Path


gdal.UseExceptions()


@op
def generate_options_str(gcp: np.ndarray) -> str:
    options_str = "-tps -t_srs EPSG:6372 "
    for row in gcp:
        options_str += "-gcp " + " ".join(row.astype(str)) + " "
    return options_str


@op(
    ins={
        "ageb_path": In(input_manager_key="path_gpkg_manager"),
    },
    out=Out(io_manager_key="gpkg_manager"),
)
def translate_geometries_1990(
    context: OpExecutionContext, ageb_path: Path, options_1990: str, options_2000: str
) -> gpd.GeoDataFrame:
    zone = context.partition_key

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        temp_path = temp_dir / f"{zone}_temp.gpkg"
        out_path = temp_dir / f"{zone}.gpkg"

        gdal.VectorTranslate(str(temp_path), str(ageb_path), options=options_1990)
        gdal.VectorTranslate(str(out_path), str(temp_path), options=options_2000)

        os.remove(temp_path)

        gdf = gpd.read_file(out_path)
    return gdf


def load_final_gcp_factory(year: int) -> OpDefinition:
    @op(name=f"load_final_gcp_{year}")
    def _op(context: OpExecutionContext, path_resource: PathResource) -> np.ndarray:
        gcp_path = (
            Path(path_resource.manual_path)
            / f"gcp/{year}/{context.partition_key}.points"
        )
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


@op(
    ins={
        "ageb_path": In(input_manager_key="path_gpkg_manager"),
    },
    out=Out(io_manager_key="gpkg_manager"),
)
def translate_geometries_2000(
    context: OpExecutionContext, ageb_path: Path, options: str
) -> gpd.GeoDataFrame:
    with tempfile.TemporaryDirectory() as temp_dir:
        out_path = Path(temp_dir) / f"{context.partition_key}.gpkg"
        gdal.VectorTranslate(str(out_path), str(ageb_path), options=options)
        gdf = gpd.read_file(out_path)
    return gdf


# pylint: disable=no-value-for-parameter
@graph_asset(
    name="2000",
    key_prefix=["translated"],
    ins={"ageb_path": AssetIn(["zone_agebs", "shaped", "2000"])},
    partitions_def=zone_partitions,
)
def translated_2000(ageb_path: Path):
    gcp_2000 = load_final_gcp_2000()
    options_str = generate_options_str(gcp_2000)
    translated = translate_geometries_2000(ageb_path, options_str)
    return translated


# pylint: disable=no-value-for-parameter
@graph_asset(
    name="1990",
    key_prefix=["translated"],
    ins={"ageb_path": AssetIn(key=["zone_agebs", "shaped", "1990"])},
    partitions_def=zone_partitions,
)
def translated_1990(
    ageb_path: Path,
) -> None:
    gcp_1990 = load_final_gcp_1990()
    gcp_2000 = load_final_gcp_2000()

    options_1990 = generate_options_str(gcp_1990)
    options_2000 = generate_options_str(gcp_2000)

    translated = translate_geometries_1990(ageb_path, options_1990, options_2000)
    return translated
