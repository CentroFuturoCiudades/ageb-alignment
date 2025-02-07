import geopandas as gpd

from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


@asset(name="2010", key_prefix=["geometry", "loc"], io_manager_key="gpkg_manager")
def geometry_loc_2010(path_resource: PathResource):
    urb_path = Path(path_resource.raw_path) / "geometry/2010/mglu2010v5_0/Localidades_urbanas_2010_5.shp"
    return gpd.read_file(urb_path).to_crs("EPSG:6372")


@asset(name="2020", key_prefix=["geometry", "loc"], io_manager_key="gpkg_manager")
def geometry_loc_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    in_path = Path(path_resource.raw_path) / "geometry/2020"
    return gpd.read_file(in_path / "00l.shp", engine="pyogrio").to_crs("EPSG:6372")
