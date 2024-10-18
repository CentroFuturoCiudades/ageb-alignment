import geopandas as gpd
import pandas as pd

from ageb_alignment.types import GeometryTuple
from dagster import asset, AssetIn


@asset(
    name="2000",
    key_prefix=["framework", "municipalities"],
    ins={"geometry_2000": AssetIn(key=["geometry", "2000"])},
    io_manager_key="gpkg_manager",
)
def municipalities_2000(geometry_2000: GeometryTuple) -> gpd.GeoDataFrame:
    merged = (
        geometry_2000.mun.drop(columns=["OID", "LAYER", "NOM_MUN"])
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
        "mun_2010": AssetIn(key=["2010", "mun"]),
        "geometry_2010": AssetIn(key=["geometry", "2010"]),
    },
    io_manager_key="gpkg_manager",
)
def municipalities_2010(
    geometry_2010: GeometryTuple, mun_2010: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_2010.mun.drop(columns=["OID", "NOM_MUN"])
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
        "mun_2020": AssetIn(key=["2020", "mun"]),
        "geometry_2020": AssetIn(key=["geometry", "2020"]),
    },
    io_manager_key="gpkg_manager",
)
def municipalities_2020(
    geometry_2020: GeometryTuple, mun_2020: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_2020.mun.drop(columns="NOMGEO")
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
