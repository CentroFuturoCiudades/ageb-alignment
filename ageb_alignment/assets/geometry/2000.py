import geopandas as gpd

from ageb_alignment.assets.geometry.common import fix_overlapped
from ageb_alignment.resources import AgebListResource, PathResource
from dagster import asset, graph_asset, op
from pathlib import Path


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
            CVEGEO=lambda df: df.CLVAGB.str.replace("-", ""),
            CVE_ENT=lambda df: df.CVEGEO.str[0:2].astype(int),
            CVE_MUN=lambda df: df.CVEGEO.str[2:5].astype(int),
            CVE_LOC=lambda df: df.CVEGEO.str[5:9].astype(int),
            CVE_AGEB=lambda df: df.CVEGEO.str[9:],
        )
        .drop(columns="CLVAGB")
        .set_index("CVEGEO")
        .sort_index()
    )
    return mg_2000_au


@op
def fix_overlapped_2000(
    overlap_resource: AgebListResource, agebs: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    if overlap_resource.ageb_2000 is not None:
        agebs = fix_overlapped(agebs, overlap_resource.ageb_2000)
    return agebs


# pylint: disable=no-value-for-parameter
@graph_asset(name="2000", key_prefix=["geometry", "ageb"])
def geometry_ageb_2000() -> gpd.GeoDataFrame:
    agebs = load_agebs_2000()
    agebs = fix_overlapped_2000(agebs)
    return agebs


@asset(name="2000", key_prefix=["geometry", "mun"])
def geometry_mun_2000(path_resource: PathResource) -> gpd.GeoDataFrame:
    mun_path = (
        Path(path_resource.raw_path) / "geometry/2000/mgm2000/Municipios_2000.shp"
    )
    mg_2000_m = gpd.read_file(mun_path).to_crs("EPSG:6372")
    return mg_2000_m


@asset(name="2000", key_prefix=["geometry", "state"])
def geometry_state_2000(path_resource: PathResource) -> gpd.GeoDataFrame:
    state_path = (
        Path(path_resource.raw_path) / "geometry/2000/mge2000/Entidades_2000.shp"
    )
    mg_2000_e = gpd.read_file(state_path).to_crs("EPSG:6372")
    return mg_2000_e
