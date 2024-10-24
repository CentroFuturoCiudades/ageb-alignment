import tempfile

import geopandas as gpd

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.assets.translate.common import (
    generate_options_str,
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
