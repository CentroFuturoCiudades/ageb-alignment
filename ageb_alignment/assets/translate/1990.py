import os
import tempfile

import geopandas as gpd

from ageb_alignment.assets.translate.common import (
    generate_options_str,
    load_final_gcp_1990,
    load_final_gcp_2000,
)
from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource
from dagster import asset, AssetExecutionContext, AssetIn
from osgeo import gdal
from pathlib import Path

gdal.UseExceptions()


def translate_geometries_1990(
    ageb_path: Path, options_1990: str, options_2000: str, partition_key: str
) -> gpd.GeoDataFrame:

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        temp_path = temp_dir / f"{partition_key}_temp.gpkg"
        out_path = temp_dir / f"{partition_key}.gpkg"

        gdal.VectorTranslate(str(temp_path), str(ageb_path), options=options_1990)
        gdal.VectorTranslate(str(out_path), str(temp_path), options=options_2000)

        os.remove(temp_path)

        gdf = gpd.read_file(out_path)
    return gdf


# pylint: disable=no-value-for-parameter
@asset(
    name="1990",
    key_prefix=["zone_agebs", "translated"],
    ins={
        "ageb_path": AssetIn(
            key=["zone_agebs", "shaped", "1990"], input_manager_key="path_gpkg_manager"
        )
    },
    partitions_def=zone_partitions,
    io_manager_key="gpkg_manager",
)
def translated_1990(
    context: AssetExecutionContext,
    path_resource: PathResource,
    ageb_path: Path,
) -> None:
    zone = context.partition_key
    manual_path = Path(path_resource.manual_path)

    gcp_1990 = load_final_gcp_1990(manual_path, zone)
    gcp_2000 = load_final_gcp_2000(manual_path, zone)

    options_1990 = generate_options_str(gcp_1990)
    options_2000 = generate_options_str(gcp_2000)

    translated = translate_geometries_1990(ageb_path, options_1990, options_2000, zone)
    return translated
