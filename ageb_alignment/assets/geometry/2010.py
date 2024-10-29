import geopandas as gpd

from ageb_alignment.assets.geometry.common import fix_overlapped_op_factory
from ageb_alignment.resources import PathResource
from dagster import asset, graph_asset, op
from pathlib import Path


@asset(name="2010", key_prefix=["geometry", "state"])
def geometry_state_2010(path_resource: PathResource) -> gpd.GeoDataFrame:
    in_path = Path(path_resource.raw_path) / "geometry/2010/"
    return gpd.read_file(in_path / "mge2010v5_0/Entidades_2010_5.shp").to_crs(
        "EPSG:6372"
    )


@asset(name="2010", key_prefix=["geometry", "mun"])
def geometry_mun_2010(path_resource: PathResource) -> gpd.GeoDataFrame:
    in_path = Path(path_resource.raw_path) / "geometry/2010/"
    return gpd.read_file(in_path / "mgm2010v5_0/Municipios_2010_5.shp").to_crs(
        "EPSG:6372"
    )


@op
def load_agebs_2010(path_resource: PathResource):
    in_path = Path(path_resource.raw_path) / "geometry/2010/"
    agebs = (
        gpd.read_file(in_path / "mgau2010v5_0/AGEB_urb_2010_5.shp")
        .to_crs("EPSG:6372")
        .set_index("CVEGEO")
    )
    return agebs


fix_overlapped_2010 = fix_overlapped_op_factory(2010)


# pylint: disable=no-value-for-parameter
@graph_asset(name="2010", key_prefix=["geometry", "ageb"])
def geometry_ageb_2010() -> gpd.GeoDataFrame:
    agebs = load_agebs_2010()
    agebs = fix_overlapped_2010(agebs)
    return agebs
