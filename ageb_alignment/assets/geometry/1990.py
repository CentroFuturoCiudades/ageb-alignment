import geopandas as gpd
import pandas as pd

from ageb_alignment.assets.geometry.common import fix_overlapped
from ageb_alignment.types import GeometryTuple
from ageb_alignment.resources import AgebEnumResource, PathResource
from dagster import asset
from pathlib import Path


def remove_slivers(gdf, ageb_list):
    # make copy
    gdf = gdf.copy()

    # Get partial gdf with slivers
    with_slivers = gdf.loc[ageb_list].copy()

    # Explode and keep larger part
    larger = (
        with_slivers.explode()
        .assign(AREA=lambda df: df.area)
        .sort_values("AREA", ascending=False)
        .groupby("CVEGEO")
        .first()
    )

    # Assign them back
    gdf.loc[ageb_list] = larger

    return gdf


def reassign_doubles(gdf, ageb_dict):
    """Explodes multipoligons and left and right parts to CVEGEO specified on lists."""
    # make copu
    gdf = gdf.copy()

    # Explode and assign new CVEGEO to each part
    doubles_idx = list(ageb_dict.keys())
    new_cvgeos = sum(ageb_dict.values(), [])

    exploded = (
        gdf.loc[doubles_idx]
        .explode()
        .assign(cx=lambda df: df.centroid.x)
        .sort_values(by="cx")
        .assign(NEW_CVEGEO=new_cvgeos, CVE_AGEB=lambda df: df.NEW_CVEGEO.str[9:])
        .drop(columns="cx")
        .set_index("NEW_CVEGEO")
        .rename_axis("CVEGEO")
    )

    # If new cvegeo is other and already exists, make union with existing one
    exists_other = [
        i for i in new_cvgeos if ((i in gdf.index) and (i not in doubles_idx))
    ]
    exploded.loc[exists_other, "geometry"] = (
        exploded.loc[exists_other].union(gdf.loc[exists_other], align=True).values
    )

    new_gdf = pd.concat(
        [gdf.drop(doubles_idx + exists_other).copy(), exploded]
    ).sort_index()

    return new_gdf


def remove_overlapped(gdf, ageb_dict):
    """Explodes multipoligons and keeps only the part specified in dict, either left
    or right."""
    # make copu
    gdf = gdf.copy()

    idxs = list(ageb_dict.keys())
    exploded = (
        gdf.loc[idxs].explode().assign(cx=lambda df: df.centroid.x).sort_values(by="cx")
    )

    idx_map = {"left": 0, "right": 1}
    new_geoms = pd.concat(
        [
            exploded.loc[idx].iloc[idx_map[x] : idx_map[x] + 1]
            for idx, x in ageb_dict.items()
        ]
    )

    gdf.loc[new_geoms.index] = new_geoms

    return gdf.sort_index()


def fix_multipoly(gdf):
    """Removes manually identified multipoligons from gdf.

    Considers three cases, a multipolygon in which a part is a small excess or sliver,a multipolygon incorrectly composed of two different Agebs, and a multipoligon composed of an ageb and an extra part that overlaps other ageb.
    """
    gdf = gdf.copy()

    with_sliver = [
        "0710800010434",
        "080320001041A",
        "0803700010755",
        "0803700011838",
        "1101500010209",
        "1406500010030",
        "1903900011633",
        "2114900010066",
        "2402700010141",
        "2500600010235",
        "3020400010162",
    ]

    gdf = remove_slivers(gdf, with_sliver)

    doubles = {
        # assign geometries to zone, if other exists do union,
        # list have [left_cvego, right_cvegeo]
        "0600200010638": ["0600200010638", "0600200010534"],
        "1410100010363": ["1410100010363", "1410100010151"],
        "280270001181A": ["280270001181A", "2802700011449"],
    }

    gdf = reassign_doubles(gdf, doubles)

    overlapped = {  ## keep stated part
        "1003200011241": "left",
        "2800300010843": "right",
        "3004800730058": "right",
    }

    gdf = remove_overlapped(gdf, overlapped)

    return gdf


def substitute_agebs(agebs_1990: gpd.GeoDataFrame, agebs_2000: gpd.GeoDataFrame):
    replace_list = [
        # Tijuana
        "0200402832683",
        "0200402832679",
        "0200402832698",
    ]

    fixed_agebs = agebs_1990.copy()
    fixed_agebs.loc[replace_list, "geometry"] = agebs_2000.loc[replace_list, "geometry"]
    return fixed_agebs


@asset
def geometry_1990(
    path_resource: PathResource, overlap_resource: AgebEnumResource, geometry_2000: GeometryTuple
) -> GeometryTuple:
    agebs_path = Path(path_resource.raw_path) / "geometry/1990/AGEB_s_90_aj.shp"
    mg_1990_au = (
        gpd.read_file(agebs_path)
        .drop(columns=["OBJECTID"])
        .assign(
            CVEGEO=lambda df: df.CVE_ENT + df.CVE_MUN + df.CVE_LOC + df.CVE_AGEB,
            CVE_ENT=lambda df: df.CVE_ENT.astype(int),
            CVE_MUN=lambda df: df.CVE_MUN.astype(int),
            CVE_LOC=lambda df: df.CVE_LOC.astype(int),
        )
        .set_index("CVEGEO")
        .drop("2405600010073")
        .sort_index()
        .to_crs("EPSG:6372")
    )

    # Remove multipoligons from 1990 geometries by fixing geometric issues
    mg_1990_au = fix_multipoly(mg_1990_au)

    # Substitute AGEBs
    mg_1990_au = substitute_agebs(mg_1990_au, geometry_2000.ageb)

    if overlap_resource.ageb_1990 is not None:
        mg_1990_au = fix_overlapped(mg_1990_au, overlap_resource.ageb_1990)

    return GeometryTuple(ageb=mg_1990_au)
