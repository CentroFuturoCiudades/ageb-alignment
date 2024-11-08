import geopandas as gpd
import pandas as pd

from ageb_alignment.assets.translate.common import (
    generate_options_str,
    get_gcp_fallback,
    translate_geometries_single,
)
from ageb_alignment.configs.replacement import replace_2000_2010
from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import AgebDictResource, PathResource
from dagster import asset, AssetExecutionContext, AssetIn
from pathlib import Path


# pylint: disable=no-value-for-parameter
@asset(
    name="2000",
    key_prefix=["zone_agebs", "translated"],
    ins={
        "ageb_path": AssetIn(
            ["zone_agebs", "shaped", "2000"], input_manager_key="path_gpkg_manager"
        ),
        "gcp_automatic_2000": AssetIn(key=["gcp", "2000"]),
    },
    partitions_def=zone_partitions,
    io_manager_key="gpkg_manager",
)
def translated_2000(
    context: AssetExecutionContext,
    path_resource: PathResource,
    affine_resource: AgebDictResource,
    ageb_path: Path,
    gcp_automatic_2000: pd.DataFrame,
) -> gpd.GeoDataFrame:
    zone = context.partition_key

    if affine_resource.ageb_2000 is not None and zone in affine_resource.ageb_2000:
        translated = gpd.read_file(ageb_path)
        translated["geometry"] = translated["geometry"].affine_transform(
            affine_resource.ageb_2000[zone]
        )
        context.log.info(f"Performed affine transformation on {zone}.")

    elif zone in replace_2000_2010:
        translated = gpd.read_file(ageb_path)
        context.log.info(f"Skipped translation for {zone}/2000.")

    else:
        gcp_final_path_2000 = (
            Path(path_resource.manual_path) / f"gcp/2000/{zone}.points"
        )
        gcp_2000, transform_options = get_gcp_fallback(
            gcp_final_path_2000, gcp_automatic_2000, context, 2000
        )

        options_str = generate_options_str(gcp_2000, transform_options)
        translated = translate_geometries_single(ageb_path, options_str)
    return translated
