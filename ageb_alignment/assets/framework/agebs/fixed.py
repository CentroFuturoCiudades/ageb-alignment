import geopandas as gpd

from ageb_alignment.assets.framework.agebs.initial import merge_1990_agebs
from ageb_alignment.resources import PathResource
from ageb_alignment.types import GeometryTuple
from dagster import (
    asset,
    graph_asset,
    op,
    AssetExecutionContext,
    AssetIn,
    OpExecutionContext,
    Out,
)
from pathlib import Path
from typing import Optional


def agebs_manual_factory(year: int) -> asset:
    @asset(
        name=str(year),
        key_prefix=["framework", "agebs"],
        ins={"agebs": AssetIn(key=["agebs_initial", str(year)])},
        io_manager_key="gpkg_manager",
    )
    def _asset(
        context: AssetExecutionContext,
        path_resource: PathResource,
        agebs: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        manual_path = Path(path_resource.intermediate_path) / f"replacement/{year}.gpkg"
        if manual_path.exists():
            df_replacement = gpd.read_file(manual_path).set_index("CVEGEO")
            agebs.update(df_replacement)
            context.log.info(f"Replaced {len(df_replacement)} AGEBs.")
        return agebs

    return _asset


agebs_assets = [agebs_manual_factory(year) for year in (2000, 2010, 2020)]


@op
def load_manual_agebs_1990(path_resource: PathResource) -> Optional[gpd.GeoDataFrame]:
    manual_path = Path(path_resource.intermediate_path) / "replacement/1990.gpkg"
    if manual_path.exists():
        return gpd.read_file(manual_path).set_index("CVEGEO")


@op(out=Out(io_manager_key="gpkg_manager"))
def replace_manual_1990(
    context: OpExecutionContext,
    merged: gpd.GeoDataFrame,
    df_replacement: Optional[gpd.GeoDataFrame],
):
    if df_replacement is not None:
        merged.update(df_replacement)
        context.log.info(f"Replaced {len(df_replacement)} AGEBs.")
    return merged


# pylint: disable=no-value-for-parameter
@graph_asset(
    name="1990",
    key_prefix=["framework", "agebs"],
    ins={
        "ageb_1990": AssetIn(key=["1990", "ageb"]),
        "geometry_1990": AssetIn(key=["geometry", "1990"]),
    },
)
def framework_agebs_1990(
    ageb_1990: gpd.GeoDataFrame,
    geometry_1990: GeometryTuple,
) -> gpd.GeoDataFrame:
    merged = merge_1990_agebs(geometry_1990, ageb_1990)
    df_replacement = load_manual_agebs_1990()
    merged = replace_manual_1990(merged, df_replacement)
    return merged
