import geopandas as gpd
import pandas as pd

from ageb_alignment.assets.translate.common import (
    generate_options_str,
    load_gcp,
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
        ),
        "gcp_orig_2000": AssetIn(key=["gcp", "2000"]),
    },
    partitions_def=zone_partitions,
    io_manager_key="gpkg_manager",
)
def translated_2000(
    context: AssetExecutionContext,
    path_resource: PathResource,
    ageb_path: Path,
    gcp_orig_2000: pd.DataFrame,
) -> gpd.GeoDataFrame:
    zone = context.partition_key
    if zone in replace_2000_2010:
        translated = gpd.read_file(ageb_path)
        context.log.info(f"Skipped translation for {zone}.")
    else:
        manual_path = Path(path_resource.manual_path)

        gcp_final_path_2000 = manual_path / f"gcp/2000/{zone}.points"
        if gcp_final_path_2000.exists():
            gcp_2000 = load_gcp(gcp_final_path_2000)
        else:
            context.log.warning("Used automatic GCP for 2000.")
            gcp_2000 = gcp_orig_2000[["sourceX", "sourceY", "mapX", "mapY"]].to_numpy()

        options_str = generate_options_str(gcp_2000)
        translated = translate_geometries_single(ageb_path, options_str)
    return translated
