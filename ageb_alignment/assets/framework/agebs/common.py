from pathlib import Path

import geopandas as gpd

from ageb_alignment.resources import PathResource
from dagster import (
    AssetIn,
    AssetsDefinition,
    OpDefinition,
    OpExecutionContext,
    Out,
    graph_asset,
    op,
)


def load_manual_agebs_factory(year: int) -> OpDefinition:
    @op(name=f"load_manual_agebs_{year}")
    def _op(
        context: OpExecutionContext,
        path_resource: PathResource,
    ) -> gpd.GeoDataFrame | None:
        manual_path = Path(path_resource.manual_path) / f"framework_replace/{year}.gpkg"
        if manual_path.exists():
            context.log.info(f"Loaded framework replace DataFrame for {year}.")
            return gpd.read_file(manual_path).set_index("CVEGEO")

        return None

    return _op


@op(out=Out(io_manager_key="gpkg_manager"))
def replace_manual_agebs(
    context: OpExecutionContext,
    merged: gpd.GeoDataFrame,
    df_replacement: gpd.GeoDataFrame | None,
) -> gpd.GeoDataFrame:
    if df_replacement is not None:
        merged.update(df_replacement)
        context.log.info(f"Replaced {len(df_replacement)} AGEBs.")
    return merged


# pylint: disable=no-value-for-parameter
def framework_agebs_factory(year: int, merge_op: OpDefinition) -> AssetsDefinition:
    load_op = load_manual_agebs_factory(year)

    @graph_asset(
        name=str(year),
        key_prefix=["framework", "agebs"],
        ins={
            "ageb": AssetIn(key=["census", str(year), "ageb"]),
            "geometry_ageb": AssetIn(key=["geometry", "ageb", str(year)]),
        },
    )
    def _asset(
        ageb: gpd.GeoDataFrame,
        geometry_ageb: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        merged = merge_op(geometry_ageb, ageb)
        df_replacement = load_op()
        merged = replace_manual_agebs(merged, df_replacement)
        return merged

    return _asset
