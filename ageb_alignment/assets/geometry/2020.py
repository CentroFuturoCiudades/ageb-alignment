import geopandas as gpd

from ageb_alignment.assets.geometry.common import fix_overlapped_op_factory
from ageb_alignment.resources import PathResource
from dagster import asset, op
from pathlib import Path


@asset(name="2020", key_prefix=["geometry", "state"])
def geometry_state_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    in_path = Path(path_resource.raw_path) / "geometry/2020"
    return gpd.read_file(in_path / "00ent.shp", engine="pyogrio").to_crs("EPSG:6372")


@asset(name="2020", key_prefix=["geometry", "mun"])
def geometry_mun_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    in_path = Path(path_resource.raw_path) / "geometry/2020"
    return gpd.read_file(in_path / "00mun.shp", engine="pyogrio").to_crs("EPSG:6372")


@op
def load_agebs_2020(path_resource: PathResource):
    in_path = Path(path_resource.raw_path) / "geometry/2020"
    agebs = (
        gpd.read_file(in_path / "00a.shp", engine="pyogrio")
        .to_crs("EPSG:6372")
        .query("Ambito == 'Urbana'")
        .set_index("CVEGEO")
    )
    return agebs


fix_overlapped_2020 = fix_overlapped_op_factory(2020)


# pylint: disable=no-value-for-parameter
@asset(name="2020", key_prefix=["geometry", "ageb"])
def geometry_ageb_2020() -> gpd.GeoDataFrame:
    agebs = load_agebs_2020()
    agebs = fix_overlapped_2020(agebs)
    return agebs
