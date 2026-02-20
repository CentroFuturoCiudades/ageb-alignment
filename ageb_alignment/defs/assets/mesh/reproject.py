from pathlib import Path
from typing import assert_never

import geopandas as gpd
import numpy as np
import pandas as pd

import dagster as dg
from ageb_alignment.defs.partitions import zone_partitions
from ageb_alignment.defs.resources import PathResource, PreferenceResource


@dg.op
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
    mesh_list: list[gpd.GeoDataFrame] = []
    for path in mesh_root_path.glob(f"nivel{preference_resource.mesh_level}*.shp"):
        df = gpd.read_file(path, engine="pyogrio", bbox=final_bounds)
        mesh_list.append(df)

    mesh = pd.concat(mesh_list, ignore_index=True).pipe(
        gpd.GeoDataFrame,
        geometry="geometry",
        crs=mesh_list[0].crs,
    )
    if "CODIGO" in mesh.columns:
        mesh = mesh.rename(columns={"CODIGO": "codigo"})
    return mesh


def reproject_to_mesh(
    mesh: gpd.GeoDataFrame,
    agebs: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    if mesh.crs != agebs.crs:
        err = f"CRS mismatch: mesh CRS is {mesh.crs}, but agebs CRS is {agebs.crs}"
        raise ValueError(err)

    agebs = agebs.copy()
    agebs["ageb_area"] = agebs.area
    intersection: gpd.GeoDataFrame = mesh.overlay(agebs, how="intersection")
    intersection["pop_fraction"] = (
        intersection.area / intersection["ageb_area"] * intersection["POBTOT"]
    )
    return (
        intersection.groupby("codigo")["pop_fraction"]
        .sum()
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
    else:
        assert_never(year)

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
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:  # pyright: ignore[reportInvalidTypeForm]
    if "reprojected_1990" in context.selected_output_names:
        yield dg.Output(reproject_to_mesh(mesh, agebs_1990), "reprojected_1990")  # pyright: ignore[reportReturnType]

    if "reprojected_2000" in context.selected_output_names:
        yield dg.Output(reproject_to_mesh(mesh, agebs_2000), "reprojected_2000")  # pyright: ignore[reportReturnType]

    if "reprojected_2010" in context.selected_output_names:
        yield dg.Output(reproject_to_mesh(mesh, agebs_2010), "reprojected_2010")  # pyright: ignore[reportReturnType]

    if "reprojected_2020" in context.selected_output_names:
        yield dg.Output(reproject_to_mesh(mesh, agebs_2020), "reprojected_2020")  # pyright: ignore[reportReturnType]


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
