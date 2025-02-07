import geopandas as gpd
import pandas as pd

from dagster import asset, AssetIn


@asset(
    name="2000",
    key_prefix=["framework", "municipalities"],
    ins={"geometry_mun_2000": AssetIn(key=["geometry", "mun", "2000"])},
    io_manager_key="gpkg_manager",
)
def municipalities_2000(geometry_mun_2000: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    merged = (
        geometry_mun_2000.drop(columns=["OID", "LAYER", "NOM_MUN"])
        .rename(columns={"CVEMUNI": "CVEGEO"})
        .assign(
            CVE_ENT=lambda df: df.CVEGEO.str[:2].astype(int),
            CVE_MUN=lambda df: df.CVEGEO.str[2:].astype(int),
            CVEGEO=lambda df: df.CVEGEO.astype(int),
        )
        .set_index("CVEGEO")
        .sort_index()
    )
    return merged


@asset(
    name="2010",
    key_prefix=["framework", "municipalities"],
    ins={
        "mun_2010": AssetIn(key=["census", "2010", "mun"]),
        "geometry_mun_2010": AssetIn(key=["geometry", "mun", "2010"]),
    },
    io_manager_key="gpkg_manager",
)
def municipalities_2010(
    geometry_mun_2010: gpd.GeoDataFrame, mun_2010: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_mun_2010.drop(columns=["OID", "NOM_MUN"])
        .assign(
            CVEGEO=lambda df: (df.CVE_ENT + df.CVE_MUN).astype(int),
            CVE_ENT=lambda df: df.CVE_ENT.astype(int),
            CVE_MUN=lambda df: df.CVE_MUN.astype(int),
        )
        .set_index("CVEGEO")
        .sort_index()
        .join(mun_2010)
    )
    return merged


@asset(
    name="2020",
    key_prefix=["framework", "municipalities"],
    ins={
        "mun_2020": AssetIn(key=["census", "2020", "mun"]),
        "geometry_mun_2020": AssetIn(key=["geometry", "mun", "2020"]),
    },
    io_manager_key="gpkg_manager",
)
def municipalities_2020(
    geometry_mun_2020: gpd.GeoDataFrame, mun_2020: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_mun_2020.drop(columns="NOMGEO")
        .assign(
            CVEGEO=lambda df: df.CVEGEO.astype(int),
            CVE_ENT=lambda df: df.CVE_ENT.astype(int),
            CVE_MUN=lambda df: df.CVE_MUN.astype(int),
        )
        .set_index("CVEGEO")
        .sort_index()
        .join(mun_2020)
    )
    return merged
