from pathlib import Path
from typing import assert_never

import geopandas as gpd
import numpy as np
import pandas as pd
from dagster_components.partitions import zone_partitions

import dagster as dg
from ageb_alignment.defs.partitions import zone_partitions
from dagster_components.resources import PostGISResource


@dg.op
def load_mesh(
    context: dg.AssetExecutionContext,
    postgis_resource: PostGISResource
) -> gpd.GeoDataFrame:
    print(context.partition_key)
    with postgis_resource.connect() as conn:
        return gpd.read_postgis(
            """
            SELECT mesh_level_9.* FROM mesh_level_9
            INNER JOIN census_2020_mun
                ON ST_Intersects(mesh_level_9.geometry, census_2020_mun.geometry)
            WHERE census_2020_mun."CVE_MET" = %(zone)s
            """,
            conn,
            params={"zone": context.partition_key},
            geom_col="geometry"
        )


def reproject_to_mesh(
    mesh: gpd.GeoDataFrame,
    agebs: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    crs = agebs.crs
    if crs is None:
        err = "CRS of agebs GeoDataFrame is not defined. Cannot reproject to mesh."
        raise ValueError(err)

    mesh = mesh.to_crs(crs)

    agebs = agebs.copy()
    agebs["ageb_area"] = agebs.area
    intersection: gpd.GeoDataFrame = mesh.overlay(agebs, how="intersection").assign(
        area_frac=lambda df: df.area.div(df["ageb_area"]),
        pop_fraction=lambda df: df["area_frac"].mul(df["POBTOT"]),
        # P_12YMAS_fraction=lambda df: df["area_frac"].mul(df["P_12YMAS"]),
    )

    return (
        intersection.groupby("codigo")
        .agg(
            {
                "pop_fraction": "sum",
                # "P_12YMAS_fraction": "sum",
            },
        )
        .reset_index()
        .merge(mesh, how="inner", on="codigo")
        .pipe(gpd.GeoDataFrame, crs=mesh.crs, geometry="geometry")
    )


ins, outs, op_outs = {}, {}, {}
for year in (1990, 2000, 2010, 2020):
    if year in (1990, 2000):
        ins[f"agebs_{year}"] = dg.AssetIn(key=["zone_agebs", "translated", str(year)])
    elif year in (2010, 2020):
        ins[f"agebs_{year}"] = dg.AssetIn(key=["zone_agebs", "shaped", str(year)])

    outs[f"reprojected_{year}"] = dg.AssetOut(
        key=["reprojected", "base", str(year)],
        io_manager_key="gpkg_manager",
    )
    op_outs[f"reprojected_{year}"] = dg.Out(
        is_required=False,
        io_manager_key="gpkg_manager",
    )


@dg.op(out=op_outs)
def reprojected_dispatcher(
    context: dg.OpExecutionContext,
    mesh: gpd.GeoDataFrame,
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
) -> tuple[
    gpd.GeoDataFrame,
    gpd.GeoDataFrame,
    gpd.GeoDataFrame,
    gpd.GeoDataFrame,
]:
    if "reprojected_1990" in context.selected_output_names:
        yield dg.Output(
            reproject_to_mesh(mesh, agebs_1990),
            "reprojected_1990",
        )

    if "reprojected_2000" in context.selected_output_names:
        yield dg.Output(
            reproject_to_mesh(mesh, agebs_2000),
            "reprojected_2000",
        )

    if "reprojected_2010" in context.selected_output_names:
        yield dg.Output(
            reproject_to_mesh(mesh, agebs_2010),
            "reprojected_2010",
        )

    if "reprojected_2020" in context.selected_output_names:
        yield dg.Output(
            reproject_to_mesh(mesh, agebs_2020),
            "reprojected_2020",
        )


@dg.graph_multi_asset(
    ins=ins,
    partitions_def=zone_partitions,
    outs=outs,
    can_subset=True,
    group_name="reprojected_base",
)
def reprojected(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
):
    mesh = load_mesh()
    reprojected_1990, reprojected_2000, reprojected_2010, reprojected_2020 = (
        reprojected_dispatcher(mesh, agebs_1990, agebs_2000, agebs_2010, agebs_2020)
    )
    return {
        "reprojected_1990": reprojected_1990,
        "reprojected_2000": reprojected_2000,
        "reprojected_2010": reprojected_2010,
        "reprojected_2020": reprojected_2020,
    }
