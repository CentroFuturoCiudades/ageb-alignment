from pathlib import Path

import geopandas as gpd

import dagster as dg
from ageb_alignment.defs.assets.geometry.agebs.common import fix_overlapped_op_factory
from ageb_alignment.defs.resources import PathResource

fix_overlapped_2000 = fix_overlapped_op_factory(2000)


@dg.op
def load_agebs_2000(path_resource: PathResource) -> gpd.GeoDataFrame:
    ageb_path = (
        Path(path_resource.data_path)
        / "initial"
        / "geometry"
        / "2000"
        / "mgau2000"
        / "agebs_urb_2000.dbf"
    )
    return (
        gpd.read_file(ageb_path)
        .to_crs("EPSG:6372")
        .drop(columns=["LAYAGB", "OID_1"])
        .assign(
            CVEGEO=lambda df: (
                df["CLVAGB"].astype(str).str.replace("-", "").str.zfill(13)
            ),
            CVE_ENT=lambda df: df["CVEGEO"].str[0:2],
            CVE_MUN=lambda df: df["CVEGEO"].str[2:5],
            CVE_LOC=lambda df: df["CVEGEO"].str[5:9],
            CVE_AGEB=lambda df: df["CVEGEO"].str[9:],
        )
        .drop(columns="CLVAGB")
        .set_index("CVEGEO")
        .sort_index()
    )


@dg.graph_asset(key=["geometry", "ageb", "2000"], group_name="geometry_agebs")
def geometry_ageb_2000() -> gpd.GeoDataFrame:
    agebs = load_agebs_2000()
    return fix_overlapped_2000(agebs)
