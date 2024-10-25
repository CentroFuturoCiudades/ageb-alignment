import os
import tempfile

import geopandas as gpd

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.assets.translate.common import (
    generate_options_str,
    load_final_gcp_1990,
    load_final_gcp_2000,
)
from dagster import graph_asset, op, AssetIn, In, OpExecutionContext, Out
from osgeo import gdal
from pathlib import Path

gdal.UseExceptions()


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


# pylint: disable=no-value-for-parameter
@graph_asset(
    name="1990",
    key_prefix=["zone_agebs", "translated"],
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
