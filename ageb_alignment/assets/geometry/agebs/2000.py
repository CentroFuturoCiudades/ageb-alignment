from pathlib import Path

import geopandas as gpd

from ageb_alignment.assets.geometry.agebs.common import fix_overlapped_op_factory
from ageb_alignment.resources import PathResource
from dagster import graph_asset, op

fix_overlapped_2000 = fix_overlapped_op_factory(2000)


@op
def load_agebs_2000(path_resource: PathResource) -> gpd.GeoDataFrame:
    ageb_path = (
        Path(path_resource.raw_path) / "geometry/2000/mgau2000/agebs_urb_2000.dbf"
    )
    mg_2000_au = (
        gpd.read_file(ageb_path)
        .to_crs("EPSG:6372")
        .drop(columns=["LAYAGB", "OID_1"])
        .assign(
            CVEGEO=lambda df: df["CLVAGB"]
            .astype(str)
            .str.replace("-", "")
            .str.zfill(13),
            CVE_ENT=lambda df: df["CVEGEO"].str[0:2],
            CVE_MUN=lambda df: df["CVEGEO"].str[2:5],
            CVE_LOC=lambda df: df["CVEGEO"].str[5:9],
            CVE_AGEB=lambda df: df["CVEGEO"].str[9:],
        )
        .drop(columns="CLVAGB")
        .set_index("CVEGEO")
        .sort_index()
    )
    return mg_2000_au


# pylint: disable=no-value-for-parameter
@graph_asset(name="2000", key_prefix=["geometry", "ageb"])
def geometry_ageb_2000() -> gpd.GeoDataFrame:
    agebs = load_agebs_2000()
    agebs = fix_overlapped_2000(agebs)
    return agebs
