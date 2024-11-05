import geopandas as gpd
import numpy as np
import pandas as pd

from ageb_alignment.configs.replacement import replace_1990_2000, replace_1990_2010
from ageb_alignment.assets.translate.common import (
    generate_options_str,
    load_gcp,
    translate_geometries_single,
    translate_geometries_double,
)
from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import AgebDictResource, PathResource
from dagster import asset, AssetExecutionContext, AssetIn
from pathlib import Path


def get_gcp_fallback(
    gcp_path: Path, gcp_orig: pd.DataFrame, context: AssetExecutionContext
) -> np.ndarray:
    if gcp_path.exists():
        gcp = load_gcp(gcp_path)
    else:
        context.log.warning("Used automatic GCP for 1990.")
        gcp = gcp_orig[["sourceX", "sourceY", "mapX", "mapY"]].to_numpy()
    return gcp


# pylint: disable=no-value-for-parameter
@asset(
    name="1990",
    key_prefix=["zone_agebs", "translated"],
    ins={
        "ageb_path": AssetIn(
            key=["zone_agebs", "shaped", "1990"], input_manager_key="path_gpkg_manager"
        ),
        "gcp_automatic_1990": AssetIn(key=["gcp", "1990"]),
        "gcp_automatic_2000": AssetIn(key=["gcp", "2000"]),
    },
    partitions_def=zone_partitions,
    io_manager_key="gpkg_manager",
)
def translated_1990(
    context: AssetExecutionContext,
    path_resource: PathResource,
    affine_resource: AgebDictResource,
    ageb_path: Path,
    gcp_automatic_1990: pd.DataFrame,
    gcp_automatic_2000: pd.DataFrame,
) -> gpd.GeoDataFrame:
    zone = context.partition_key

    if affine_resource.ageb_1990 is not None and zone in affine_resource.ageb_1990:
        translated = gpd.read_file(ageb_path)
        translated["geometry"] = translated["geometry"].affine_transform(
            affine_resource.ageb_1990[zone]
        )
        context.log.info(f"Performed affine transformation on {zone}.")

    elif zone in replace_1990_2010:
        translated = gpd.read_file(ageb_path)
        context.log.info(f"Skipped translation for {zone}.")

    else:
        manual_path = Path(path_resource.manual_path)

        options = {}
        for year, gcp_automatic in zip(
            (1990, 2000), (gcp_automatic_1990, gcp_automatic_2000)
        ):
            gcp_final_path = manual_path / f"gcp/{year}/{zone}.points"
            gcp = get_gcp_fallback(gcp_final_path, gcp_automatic, context)
            options[year] = generate_options_str(gcp)

        if zone in replace_1990_2000:
            context.log.info(f"Performed single translation for {zone}.")
            translated = translate_geometries_single(ageb_path, options[2000])
        else:
            translated = translate_geometries_double(
                ageb_path, options[1990], options[2000]
            )
    return translated
