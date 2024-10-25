import tempfile

import geopandas as gpd

from ageb_alignment.assets.translate.common import (
    generate_options_str,
    load_final_gcp_2000,
)
from ageb_alignment.configs.replacement import replace_2000_2010
from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource
from dagster import asset, AssetExecutionContext, AssetIn
from osgeo import gdal
from pathlib import Path

gdal.UseExceptions()


def translate_geometries_2000(
    ageb_path: Path, options: str, partition_key: str
) -> gpd.GeoDataFrame:
    with tempfile.TemporaryDirectory() as temp_dir:
        out_path = Path(temp_dir) / f"{partition_key}.gpkg"
        gdal.VectorTranslate(str(out_path), str(ageb_path), options=options)
        gdf = gpd.read_file(out_path)
    return gdf


# pylint: disable=no-value-for-parameter
@asset(
    name="2000",
    key_prefix=["zone_agebs", "translated"],
    ins={
        "ageb_path": AssetIn(
            ["zone_agebs", "shaped", "2000"], input_manager_key="path_gpkg_manager"
        )
    },
    partitions_def=zone_partitions,
    io_manager_key="gpkg_manager",
)
def translated_2000(
    context: AssetExecutionContext, path_resource: PathResource, ageb_path: Path
):
    zone = context.partition_key
    if zone in replace_2000_2010:
        return gpd.read_file(ageb_path)
    else:
        manual_path = Path(path_resource.manual_path)
        gcp_2000 = load_final_gcp_2000(manual_path, zone)
        options_str = generate_options_str(gcp_2000)
        translated = translate_geometries_2000(ageb_path, options_str, zone)
        return translated
