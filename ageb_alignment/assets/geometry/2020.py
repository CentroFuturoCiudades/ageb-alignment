import geopandas as gpd

from ageb_alignment.assets.geometry.common import fix_overlapped
from ageb_alignment.resources import AgebListResource, PathResource
from dagster import asset
from pathlib import Path


@asset(name="2020", key_prefix=["geometry", "state"])
def geometry_state_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    in_path = Path(path_resource.raw_path) / "geometry/2020"
    return gpd.read_file(in_path / "00ent.shp", engine="pyogrio").to_crs("EPSG:6372")


@asset(name="2020", key_prefix=["geometry", "mun"])
def geometry_mun_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    in_path = Path(path_resource.raw_path) / "geometry/2020"
    return gpd.read_file(in_path / "00mun.shp", engine="pyogrio").to_crs("EPSG:6372")


@asset(name="2020", key_prefix=["geometry", "ageb"])
def geometry_ageb_2020(
    path_resource: PathResource, overlap_resource: AgebListResource
) -> gpd.GeoDataFrame:
    in_path = Path(path_resource.raw_path) / "geometry/2020"

    agebs = (
        gpd.read_file(in_path / "00a.shp", engine="pyogrio")
        .to_crs("EPSG:6372")
        .query("Ambito == 'Urbana'")
        .set_index("CVEGEO")
    )
    if overlap_resource.ageb_2020 is not None:
        agebs = fix_overlapped(agebs, overlap_resource.ageb_2020)

    return agebs
