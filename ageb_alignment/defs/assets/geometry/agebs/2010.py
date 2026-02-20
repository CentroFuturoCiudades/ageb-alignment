from pathlib import Path

import geopandas as gpd

import dagster as dg
from ageb_alignment.defs.assets.geometry.agebs.common import fix_overlapped_op_factory
from ageb_alignment.defs.resources import PathResource

fix_overlapped_2010 = fix_overlapped_op_factory(2010)


@dg.op
def load_agebs_2010(path_resource: PathResource) -> gpd.GeoDataFrame:
    in_path = Path(path_resource.data_path) / "initial" / "geometry" / "2010"
    return (
        gpd.read_file(in_path / "mgau2010v5_0/AGEB_urb_2010_5.shp")
        .to_crs("EPSG:6372")
        .assign(CVEGEO=lambda df: df["CVEGEO"].astype(str).str.zfill(13))
        .set_index("CVEGEO")
    )


@dg.graph_asset(key=["geometry", "ageb", "2010"], group_name="geometry_agebs")
def geometry_ageb_2010() -> gpd.GeoDataFrame:
    agebs = load_agebs_2010()
    return fix_overlapped_2010(agebs)
