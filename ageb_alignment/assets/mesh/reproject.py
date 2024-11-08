import geopandas as gpd
import numpy as np
import pandas as pd

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource, PreferenceResource
from dagster import (
    graph_multi_asset,
    op,
    AssetIn,
    AssetOut,
    OpExecutionContext,
    Out,
    Output,
)
from pathlib import Path
from typing import assert_never


@op
def load_mesh(
    path_resource: PathResource,
    preference_resource: PreferenceResource,
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    all_bounds = np.empty((4, 4), dtype=float)
    for i, agebs in enumerate((agebs_1990, agebs_2000, agebs_2010, agebs_2020)):
        all_bounds[i] = agebs.to_crs("EPSG:6365").total_bounds

    final_bounds = (
        all_bounds[:, 0].min(),
        all_bounds[:, 1].min(),
        all_bounds[:, 2].max(),
        all_bounds[:, 3].max(),
    )

    mesh_root_path = Path(path_resource.raw_path) / "mesh"
    mesh = []
    for path in mesh_root_path.glob(f"nivel{preference_resource.mesh_level}*.shp"):
        df = gpd.read_file(path, engine="pyogrio", bbox=final_bounds)
        mesh.append(df)
    mesh = pd.concat(mesh, ignore_index=True)
    mesh = mesh.to_crs("EPSG:6372")
    if "CODIGO" in mesh.columns:
        mesh = mesh.rename(columns={"CODIGO": "codigo"})
    return mesh


def reproject_to_mesh(
    mesh: gpd.GeoDataFrame, agebs: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    assert mesh.crs == agebs.crs

    agebs = agebs.copy()
    agebs["ageb_area"] = agebs.area
    intersection: gpd.GeoDataFrame = mesh.overlay(agebs, how="intersection")
    intersection["pop_fraction"] = (
        intersection.area / intersection["ageb_area"] * intersection["POBTOT"]
    )
    intersection = (
        intersection.groupby("codigo")["pop_fraction"]
        .sum()
        .reset_index()
        .merge(mesh, how="inner", on="codigo")
    )
    intersection = gpd.GeoDataFrame(intersection, crs=mesh.crs, geometry="geometry")
    return intersection


ins, outs, op_outs = {}, {}, {}
for year in (1990, 2000, 2010, 2020):
    if year in (1990, 2000):
        ins[f"agebs_{year}"] = AssetIn(key=["zone_agebs", "translated", str(year)])
    elif year in (2010, 2020):
        ins[f"agebs_{year}"] = AssetIn(key=["zone_agebs", "shaped", str(year)])
    else:
        assert_never(year)

    outs[f"reprojected_{year}"] = AssetOut(
        key=["reprojected", "base", str(year)], io_manager_key="gpkg_manager"
    )
    op_outs[f"reprojected_{year}"] = Out(
        is_required=False, io_manager_key="gpkg_manager"
    )


@op(out=op_outs)
def reprojected_dispatcher(
    context: OpExecutionContext,
    mesh: gpd.GeoDataFrame,
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
):
    if "reprojected_1990" in context.selected_output_names:
        yield Output(reproject_to_mesh(mesh, agebs_1990), "reprojected_1990")

    if "reprojected_2000" in context.selected_output_names:
        yield Output(reproject_to_mesh(mesh, agebs_2000), "reprojected_2000")

    if "reprojected_2010" in context.selected_output_names:
        yield Output(reproject_to_mesh(mesh, agebs_2010), "reprojected_2010")

    if "reprojected_2020" in context.selected_output_names:
        yield Output(reproject_to_mesh(mesh, agebs_2020), "reprojected_2020")


# pylint: disable=no-value-for-parameter
@graph_multi_asset(ins=ins, partitions_def=zone_partitions, outs=outs, can_subset=True)
def reprojected(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
    agebs_2020: gpd.GeoDataFrame,
):
    mesh = load_mesh(agebs_1990, agebs_2000, agebs_2010, agebs_2020)
    reprojected_1990, reprojected_2000, reprojected_2010, reprojected_2020 = (
        reprojected_dispatcher(mesh, agebs_1990, agebs_2000, agebs_2010, agebs_2020)
    )
    return {
        "reprojected_1990": reprojected_1990,
        "reprojected_2000": reprojected_2000,
        "reprojected_2010": reprojected_2010,
        "reprojected_2020": reprojected_2020,
    }
