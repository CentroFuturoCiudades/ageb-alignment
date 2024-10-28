import geopandas as gpd

from ageb_alignment.assets.translate.common import (
    generate_options_str,
    load_final_gcp,
    translate_geometries_single,
)
from ageb_alignment.configs.replacement import replace_2000_2010
from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource
from dagster import asset, AssetExecutionContext, AssetIn
from pathlib import Path


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
) -> gpd.GeoDataFrame:
    zone = context.partition_key
    if zone in replace_2000_2010:
        translated = gpd.read_file(ageb_path)
        context.log.info(f"Skipped translation for {zone}.")
    else:
        manual_path = Path(path_resource.manual_path)
        gcp_2000 = load_final_gcp(manual_path / f"gcp/2000/{zone}.points")
        options_str = generate_options_str(gcp_2000)
        translated = translate_geometries_single(ageb_path, options_str)
    return translated
