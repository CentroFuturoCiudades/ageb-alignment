import geopandas as gpd
import pandas as pd

from ageb_alignment.configs.replacement import replace_1990_2000, replace_1990_2010
from ageb_alignment.assets.translate.common import (
    generate_options_str,
    load_gcp,
    translate_geometries_single,
    translate_geometries_double,
)
from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource
from dagster import asset, AssetExecutionContext, AssetIn
from pathlib import Path


# pylint: disable=no-value-for-parameter
@asset(
    name="1990",
    key_prefix=["zone_agebs", "translated"],
    ins={
        "ageb_path": AssetIn(
            key=["zone_agebs", "shaped", "1990"], input_manager_key="path_gpkg_manager"
        ),
        "gcp_orig_1990": AssetIn(key=["gcp", "1990"]),
        "gcp_orig_2000": AssetIn(key=["gcp", "2000"]),
    },
    partitions_def=zone_partitions,
    io_manager_key="gpkg_manager",
)
def translated_1990(
    context: AssetExecutionContext,
    path_resource: PathResource,
    ageb_path: Path,
    gcp_orig_1990: pd.DataFrame,
    gcp_orig_2000: pd.DataFrame,
) -> gpd.GeoDataFrame:
    zone = context.partition_key
    if zone in replace_1990_2010:
        translated = gpd.read_file(ageb_path)
        context.log.info(f"Skipped translation for {zone}.")
    else:
        manual_path = Path(path_resource.manual_path)

        gcp_final_path_1990 = manual_path / f"gcp/1990/{zone}.points"
        if gcp_final_path_1990.exists():
            gcp_1990 = load_gcp(gcp_final_path_1990)
        else:
            context.log.warning("Used automatic GCP for 1990.")
            gcp_1990 = gcp_orig_1990[["sourceX", "sourceY", "mapX", "mapY"]].to_numpy()

        gcp_final_path_2000 = manual_path / f"gcp/2000/{zone}.points"
        if gcp_final_path_2000.exists():
            gcp_2000 = load_gcp(gcp_final_path_2000)
        else:
            context.log.warning("Used automatic GCP for 2000.")
            gcp_2000 = gcp_orig_2000[["sourceX", "sourceY", "mapX", "mapY"]].to_numpy()

        options_1990 = generate_options_str(gcp_1990)
        options_2000 = generate_options_str(gcp_2000)

        if zone in replace_1990_2000:
            context.log.info(f"Performed single translation for {zone}.")
            translated = translate_geometries_single(ageb_path, options_2000)
        else:
            translated = translate_geometries_double(
                ageb_path, options_1990, options_2000
            )
    return translated
