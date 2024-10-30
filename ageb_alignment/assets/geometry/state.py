import geopandas as gpd

from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


@asset(name="2000", key_prefix=["geometry", "state"])
def geometry_state_2000(path_resource: PathResource) -> gpd.GeoDataFrame:
    state_path = (
        Path(path_resource.raw_path) / "geometry/2000/mge2000/Entidades_2000.shp"
    )
    mg_2000_e = gpd.read_file(state_path).to_crs("EPSG:6372")
    return mg_2000_e


@asset(name="2010", key_prefix=["geometry", "state"])
def geometry_state_2010(path_resource: PathResource) -> gpd.GeoDataFrame:
    in_path = Path(path_resource.raw_path) / "geometry/2010/"
    return gpd.read_file(in_path / "mge2010v5_0/Entidades_2010_5.shp").to_crs(
        "EPSG:6372"
    )


@asset(name="2020", key_prefix=["geometry", "state"])
def geometry_state_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    in_path = Path(path_resource.raw_path) / "geometry/2020"
    return gpd.read_file(in_path / "00ent.shp", engine="pyogrio").to_crs("EPSG:6372")
