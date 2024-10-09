import geopandas as gpd

from ageb_alignment.types import GeometryTuple
from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


@asset
def geometry_2010(path_resource: PathResource) -> GeometryTuple:
    in_path = Path(path_resource.raw_path) / "geometry/2010/"
    return GeometryTuple(
        ent=gpd.read_file(in_path / "mge2010v5_0/Entidades_2010_5.shp").to_crs(
            "EPSG:6372"
        ),
        mun=gpd.read_file(in_path / "mgm2010v5_0/Municipios_2010_5.shp").to_crs(
            "EPSG:6372"
        ),
        ageb=gpd.read_file(in_path / "mgau2010v5_0/AGEB_urb_2010_5.shp").to_crs(
            "EPSG:6372"
        ),
    )
