import dagster as dg
import geopandas as gpd
import pandas as pd

from ageb_alignment.assets.translate.common import (
    generate_options_str,
    get_gcp_fallback,
    translate_geometries_single,
)
from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource
from pathlib import Path


def translated_factory(year: int) -> dg.AssetsDefinition:
    @dg.asset(
        name=str(year),
        key_prefix=["zone_agebs", "translated"],
        ins={
            "ageb_path": dg.AssetIn(
                ["zone_agebs", "shaped", str(year)], input_manager_key="path_gpkg_manager"
            ),
            "gcp_automatic": dg.AssetIn(key=["gcp", str(year)]),
        },
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        path_resource: PathResource,
        ageb_path: Path,
        gcp_automatic: pd.DataFrame,
    ) -> gpd.GeoDataFrame:
        zone = context.partition_key

        gcp_final_path = (
            Path(path_resource.manual_path) / "gcp" / str(year) / f"{zone}.points"
        )
        gcp, transform_options = get_gcp_fallback(
            gcp_final_path, gcp_automatic, context, year
        )

        options_str = generate_options_str(gcp, transform_options)
        return translate_geometries_single(ageb_path, options_str)

    return _asset


dassets = [translated_factory(1990), translated_factory(2000)]