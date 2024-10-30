import geopandas as gpd

from ageb_alignment.assets.geometry.agebs.common import fix_overlapped_op_factory
from ageb_alignment.resources import PathResource
from dagster import graph_asset, op
from pathlib import Path


fix_overlapped_2010 = fix_overlapped_op_factory(2010)


@op
def load_agebs_2010(path_resource: PathResource):
    in_path = Path(path_resource.raw_path) / "geometry/2010/"
    agebs = (
        gpd.read_file(in_path / "mgau2010v5_0/AGEB_urb_2010_5.shp")
        .to_crs("EPSG:6372")
        .set_index("CVEGEO")
    )
    return agebs


# pylint: disable=no-value-for-parameter
@graph_asset(name="2010", key_prefix=["geometry", "ageb"])
def geometry_ageb_2010() -> gpd.GeoDataFrame:
    agebs = load_agebs_2010()
    agebs = fix_overlapped_2010(agebs)
    return agebs
