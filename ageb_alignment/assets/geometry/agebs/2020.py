import geopandas as gpd

from ageb_alignment.assets.geometry.agebs.common import fix_overlapped_op_factory
from ageb_alignment.resources import PathResource
from dagster import graph_asset, op
from pathlib import Path


fix_overlapped_2020 = fix_overlapped_op_factory(2020)


@op
def load_agebs_2020(path_resource: PathResource):
    in_path = Path(path_resource.raw_path) / "geometry/2020"
    agebs = (
        gpd.read_file(in_path / "00a.shp")
        .to_crs("EPSG:6372")
        .query("Ambito == 'Urbana'")
        .set_index("CVEGEO")
    )
    return agebs


# pylint: disable=no-value-for-parameter
@graph_asset(name="2020", key_prefix=["geometry", "ageb"])
def geometry_ageb_2020() -> gpd.GeoDataFrame:
    agebs = load_agebs_2020()
    agebs = fix_overlapped_2020(agebs)
    return agebs
