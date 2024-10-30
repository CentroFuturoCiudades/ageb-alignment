import geopandas as gpd

from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


@asset(name="2000", key_prefix=["geometry", "mun"])
def geometry_mun_2000(path_resource: PathResource) -> gpd.GeoDataFrame:
    mun_path = (
        Path(path_resource.raw_path) / "geometry/2000/mgm2000/Municipios_2000.shp"
    )
    mg_2000_m = gpd.read_file(mun_path).to_crs("EPSG:6372")
    return mg_2000_m


@asset(name="2010", key_prefix=["geometry", "mun"])
def geometry_mun_2010(path_resource: PathResource) -> gpd.GeoDataFrame:
    in_path = Path(path_resource.raw_path) / "geometry/2010/"
    return gpd.read_file(in_path / "mgm2010v5_0/Municipios_2010_5.shp").to_crs(
        "EPSG:6372"
    )


@asset(name="2020", key_prefix=["geometry", "mun"])
def geometry_mun_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    in_path = Path(path_resource.raw_path) / "geometry/2020"
    return gpd.read_file(in_path / "00mun.shp", engine="pyogrio").to_crs("EPSG:6372")
