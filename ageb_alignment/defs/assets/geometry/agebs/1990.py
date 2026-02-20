from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from ageb_alignment.defs.assets.geometry.agebs.common import fix_overlapped_op_factory
from ageb_alignment.defs.resources import PathResource

fix_overlapped_1990 = fix_overlapped_op_factory(1990)


@dg.op
def load_agebs_1990(path_resource: PathResource) -> gpd.GeoDataFrame:
    agebs_path = (
        Path(path_resource.data_path)
        / "initial"
        / "geometry"
        / "1990"
        / "AGEB_s_90_aj.shp"
    )
    return (
        gpd.read_file(agebs_path)
        .drop(columns=["OBJECTID"])
        .assign(
            CVE_ENT=lambda df: df["CVE_ENT"].astype(str).str.zfill(2),
            CVE_MUN=lambda df: df["CVE_MUN"].astype(str).str.zfill(3),
            CVE_LOC=lambda df: df["CVE_LOC"].astype(str).str.zfill(4),
            CVE_AGEB=lambda df: df["CVE_AGEB"].astype(str).str.zfill(4),
            CVEGEO=lambda df: df.CVE_ENT + df.CVE_MUN + df.CVE_LOC + df.CVE_AGEB,
        )
        .set_index("CVEGEO")
        .drop("2405600010073")
        .sort_index()
        .to_crs("EPSG:6372")
    )


@dg.op
def remove_slivers(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
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
    gdf = gdf.copy()

    # Get partial gdf with slivers
    with_slivers = gdf.loc[with_sliver].copy()

    # Explode and keep larger part
    larger = (
        with_slivers.explode()
        .assign(AREA=lambda df: df.area)
        .sort_values("AREA", ascending=False)
        .groupby("CVEGEO")
        .first()
    )

    # Assign them back
    gdf.loc[with_sliver] = larger
    return gdf


@dg.op
def reassign_doubles(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Explodes multipoligons and left and right parts to CVEGEO specified on lists."""

    doubles = {
        "0600200010638": ["0600200010638", "0600200010534"],
        "1410100010363": ["1410100010363", "1410100010151"],
        "280270001181A": ["280270001181A", "2802700011449"],
    }

    gdf = gdf.copy()

    # Explode and assign new CVEGEO to each part
    doubles_idx = list(doubles.keys())
    new_cvgeos = sum(doubles.values(), [])

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

    return gpd.GeoDataFrame(
        pd.concat(
            [gdf.drop(doubles_idx + exists_other).copy(), exploded],
        ).sort_index(),
    )


@dg.op
def remove_overlapped(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Explodes multipoligons and keeps only the part specified in dict, either left
    or right."""

    overlapped = {  ## keep stated part
        "1003200011241": "left",
        "2800300010843": "right",
        "3004800730058": "right",
    }
    gdf = gdf.copy()

    idxs = list(overlapped.keys())
    exploded = (
        gdf.loc[idxs].explode().assign(cx=lambda df: df.centroid.x).sort_values(by="cx")
    )

    idx_map = {"left": 0, "right": 1}
    new_geoms = pd.concat(
        [
            exploded.loc[idx].iloc[idx_map[x] : idx_map[x] + 1]
            for idx, x in overlapped.items()
        ],
    )

    gdf.loc[new_geoms.index] = new_geoms

    return gdf.sort_index()


@dg.op
def substitute_agebs(
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    agebs_2000 = agebs_2000.set_index("CVEGEO")

    replace_list = [
        # Tijuana
        "0200402832683",
        "0200402832679",
        "0200402832698",
    ]

    fixed_agebs = agebs_1990.copy()
    fixed_agebs.loc[replace_list, "geometry"] = agebs_2000.loc[replace_list, "geometry"]
    return fixed_agebs


@dg.graph_asset(
    key=["geometry", "ageb", "1990"],
    ins={"geometry_ageb_2000": dg.AssetIn(key=["geometry", "ageb", "2000"])},
    group_name="geometry_agebs",
)
def geometry_ageb_1990(
    geometry_ageb_2000: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    mg_1990_au = load_agebs_1990()
    mg_1990_au = remove_slivers(mg_1990_au)
    mg_1990_au = reassign_doubles(mg_1990_au)
    mg_1990_au = remove_overlapped(mg_1990_au)
    mg_1990_au = substitute_agebs(mg_1990_au, geometry_ageb_2000)
    return fix_overlapped_1990(mg_1990_au)
