import geopandas as gpd
import pandas as pd

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource, PreferenceResource
from dagster import graph_asset, op, AssetIn, In, Out
from pathlib import Path
from typing import assert_never


@op
def load_mesh(
    path_resource: PathResource,
    preference_resource: PreferenceResource,
    agebs: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    bounds = agebs.to_crs("EPSG:6365").total_bounds

    mesh_root_path = Path(path_resource.raw_path) / "mesh"
    mesh = []
    for path in mesh_root_path.glob(f"nivel{preference_resource.mesh_level}*.shp"):
        df = gpd.read_file(path, engine="pyogrio", bbox=tuple(bounds))
        mesh.append(df)
    mesh = pd.concat(mesh, ignore_index=True)
    mesh = mesh.to_crs("EPSG:6372")
    if "CODIGO" in mesh.columns:
        mesh = mesh.rename(columns={"CODIGO": "codigo"})
    return mesh


@op(
    ins={"agebs": In(input_manager_key="gpkg_manager")},
    out=Out(io_manager_key="gpkg_manager"),
)
def reproject_to_mesh(
    mesh: gpd.GeoDataFrame, agebs: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    assert mesh.crs == agebs.crs

    agebs = agebs.copy()
    agebs["ageb_area"] = agebs.area
    intersection = mesh.overlay(agebs, how="intersection")
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


# pylint: disable=no-value-for-parameter
def reprojected_factory(year: int):
    if year in (1990, 2000):
        asset_key = "translated"
    elif year in (2010, 2020):
        asset_key = "shaped"
    else:
        assert_never(year)

    @graph_asset(
        name=str(year),
        key_prefix="reprojected",
        ins={"agebs": AssetIn(key=["zone_agebs", asset_key, str(year)])},
        partitions_def=zone_partitions,
    )
    def _asset(agebs: dict[str, gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
        mesh = load_mesh(agebs)
        return reproject_to_mesh(mesh, agebs)

    return _asset


reprojected_assets = [reprojected_factory(year) for year in (1990, 2000, 2010, 2020)]
