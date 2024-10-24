import geopandas as gpd

from ageb_alignment.resources import PathResource
from ageb_alignment.types import GeometryTuple
from dagster import (
    graph_asset,
    op,
    AssetsDefinition,
    AssetIn,
    OpDefinition,
    OpExecutionContext,
    Out,
)
from pathlib import Path
from typing import Optional


def load_manual_agebs_factory(year: int) -> OpDefinition:
    @op(name=f"load_manual_agebs_{year}")
    def _op(
        context: OpExecutionContext, path_resource: PathResource
    ) -> Optional[gpd.GeoDataFrame]:
        manual_path = Path(path_resource.manual_path) / f"framework_replace/{year}.gpkg"
        if manual_path.exists():
            context.log.info(f"Loaded framework replace DataFrame for {year}.")
            return gpd.read_file(manual_path).set_index("CVEGEO")

    return _op


@op(out=Out(io_manager_key="gpkg_manager"))
def replace_manual_agebs(
    context: OpExecutionContext,
    merged: gpd.GeoDataFrame,
    df_replacement: Optional[gpd.GeoDataFrame],
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
            "ageb": AssetIn(key=[str(year), "ageb"]),
            "geometry": AssetIn(key=["geometry", str(year)]),
        },
    )
    def _asset(
        ageb: gpd.GeoDataFrame,
        geometry: GeometryTuple,
    ) -> gpd.GeoDataFrame:
        merged = merge_op(geometry, ageb)
        df_replacement = load_op()
        merged = replace_manual_agebs(merged, df_replacement)
        return merged

    return _asset
