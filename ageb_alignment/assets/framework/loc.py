import geopandas as gpd
import pandas as pd

from dagster import asset, AssetIn


@asset(
    name="2010",
    key_prefix=["framework", "loc"],
    ins={
        "loc_2010": AssetIn(key=["census", "2010", "loc"]),
        "geometry_loc_2010": AssetIn(key=["geometry", "loc", "2010"]),
    },
    io_manager_key="gpkg_manager",
)
def loc_2010(
    geometry_loc_2010: gpd.GeoDataFrame, loc_2010: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_loc_2010.drop(columns=["OID", "NOM_LOC"])
        .assign(
            CVEGEO=lambda df: (df.CVE_ENT + df.CVE_MUN + df.CVE_LOC).astype(int),
            CVE_ENT=lambda df: df.CVE_ENT.astype(int),
            CVE_MUN=lambda df: df.CVE_MUN.astype(int),
            CVE_LOC=lambda df: df.CVE_LOC.astype(int),
        )
        .set_index("CVEGEO")
        .sort_index()
        .join(
            loc_2010
            .set_index("CVEGEO")
        )
    )
    return merged


@asset(
    name="2020",
    key_prefix=["framework", "loc"],
    ins={
        "loc_2020": AssetIn(key=["census", "2020", "loc"]),
        "geometry_loc_2020": AssetIn(key=["geometry", "loc", "2020"]),
    },
    io_manager_key="gpkg_manager",
)
def loc_2020(
    geometry_loc_2020: gpd.GeoDataFrame, loc_2020: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_loc_2020.drop(columns="NOMGEO")
        .assign(
            CVEGEO=lambda df: df.CVEGEO.astype(int),
            CVE_ENT=lambda df: df.CVE_ENT.astype(int),
            CVE_MUN=lambda df: df.CVE_MUN.astype(int),
            CVE_LOC=lambda df: df.CVE_LOC.astype(int),
        )
        .set_index("CVEGEO")
        .sort_index()
        .join(
            loc_2020
            .set_index("CVEGEO")
        )
    )
    return merged
