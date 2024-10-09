import geopandas as gpd

from ageb_alignment.resources import PathResource
from ageb_alignment.types import GeometryTuple
from dagster import asset
from pathlib import Path


@asset
def geometry_2020(path_resource: PathResource) -> GeometryTuple:
    in_path = Path(path_resource.raw_path) / "geometry/2020"

    agebs = gpd.read_file(in_path / "00a.shp", engine="pyogrio").to_crs("EPSG:6372")
    agebs = agebs.query("Ambito == 'Urbana'")

    return GeometryTuple(
        ent=gpd.read_file(in_path / "00ent.shp", engine="pyogrio").to_crs("EPSG:6372"),
        mun=gpd.read_file(in_path / "00mun.shp", engine="pyogrio").to_crs("EPSG:6372"),
        ageb=agebs,
    )
