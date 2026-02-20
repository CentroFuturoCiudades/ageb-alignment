from pathlib import Path

import geopandas as gpd

import dagster as dg
from ageb_alignment.defs.assets.geometry.agebs.common import fix_overlapped_op_factory
from ageb_alignment.defs.resources import PathResource

fix_overlapped_2020 = fix_overlapped_op_factory(2020)


@dg.op
def load_agebs_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    in_path = Path(path_resource.raw_path) / "geometry/2020"
    return (
        gpd.read_file(in_path / "00a.shp")
        .to_crs("EPSG:6372")
        .query("Ambito == 'Urbana'")
        .assign(CVEGEO=lambda df: df["CVEGEO"].astype(str).str.zfill(13))
        .set_index("CVEGEO")
    )


@dg.graph_asset(key=["geometry", "ageb", "2020"], group_name="geometry_agebs")
def geometry_ageb_2020() -> gpd.GeoDataFrame:
    agebs = load_agebs_2020()
    return fix_overlapped_2020(agebs)
