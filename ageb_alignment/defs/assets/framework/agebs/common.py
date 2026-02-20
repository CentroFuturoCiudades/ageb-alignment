from pathlib import Path

import geopandas as gpd

from ageb_alignment.defs.resources import PathResource
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
        manual_path = (
            Path(path_resource.data_path)
            / "intermediate"
            / "framework_replace"
            / f"{year}.gpkg"
        )
        if manual_path.exists():
            msg = f"Loaded framework replace DataFrame for {year}."
            context.log.info(msg)
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
        msg = f"Replaced {len(df_replacement)} AGEBs."
        context.log.info(msg)
    return merged


# pylint: disable=no-value-for-parameter
def framework_agebs_factory(year: int, merge_op: OpDefinition) -> AssetsDefinition:
    load_op = load_manual_agebs_factory(year)

    @graph_asset(
        key=["framework", "agebs", str(year)],
        ins={
            "ageb": AssetIn(key=["census", str(year), "ageb"]),
            "geometry_ageb": AssetIn(key=["geometry", "ageb", str(year)]),
        },
        group_name="framework_agebs",
    )
    def _asset(
        ageb: gpd.GeoDataFrame,
        geometry_ageb: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        merged = merge_op(geometry_ageb, ageb)
        df_replacement = load_op()
        return replace_manual_agebs(merged, df_replacement)

    return _asset
