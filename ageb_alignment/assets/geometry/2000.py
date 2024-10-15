import geopandas as gpd

from ageb_alignment.assets.geometry.common import fix_overlapped
from ageb_alignment.resources import AgebListResource, PathResource
from ageb_alignment.types import GeometryTuple
from dagster import asset
from pathlib import Path


def _load_state(state_path: Path) -> gpd.GeoDataFrame:
    mg_2000_e = gpd.read_file(state_path).to_crs("EPSG:6372")
    return mg_2000_e


def _load_mun(mun_path: Path) -> gpd.GeoDataFrame:
    mg_2000_m = gpd.read_file(mun_path).to_crs("EPSG:6372")
    return mg_2000_m


def _load_agebs(agebs_path: Path) -> GeometryTuple:
    mg_2000_au = (
        gpd.read_file(agebs_path)
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


@asset(name="2000", key_prefix="geometry")
def geometry_2000(
    path_resource: PathResource, overlap_resource: AgebListResource
) -> GeometryTuple:
    in_path = Path(path_resource.raw_path) / "geometry/2000"

    agebs = _load_agebs(in_path / "mgau2000/agebs_urb_2000.dbf")
    if overlap_resource.ageb_2000 is not None:
        agebs = fix_overlapped(agebs, overlap_resource.ageb_2000)

    return GeometryTuple(
        ent=_load_state(in_path / "mge2000/Entidades_2000.shp"),
        mun=_load_mun(in_path / "mgm2000/Municipios_2000.shp"),
        ageb=agebs,
    )
