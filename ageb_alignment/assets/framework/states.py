import geopandas as gpd
import pandas as pd

from dagster import asset, AssetIn


@asset(
    name="2000",
    key_prefix=["framework", "states"],
    ins={"geometry_state_2000": AssetIn(key=["geometry", "state", "2000"])},
    io_manager_key="gpkg_manager",
)
def states_2000(geometry_state_2000: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    merged = (
        geometry_state_2000.drop(columns="OID")
        .assign(CVE_ENT=lambda df: df.CVE_ENT.astype(int))
        .set_index("CVE_ENT")
        .sort_index()
    )
    return merged


@asset(
    name="2010",
    key_prefix=["framework", "states"],
    ins={
        "state_2010": AssetIn(key=["2010", "state"]),
        "geometry_state_2010": AssetIn(key=["geometry", "state", "2010"]),
    },
    io_manager_key="gpkg_manager",
)
def states_2010(
    geometry_state_2010: gpd.GeoDataFrame, state_2010: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_state_2010.drop(columns=["OID", "NOM_ENT"])
        .assign(CVE_ENT=lambda df: df.CVE_ENT.astype(int))
        .set_index("CVE_ENT")
        .sort_index()
        .join(state_2010)
    )
    return merged


@asset(
    name="2020",
    key_prefix=["framework", "states"],
    ins={
        "state_2020": AssetIn(key=["2020", "state"]),
        "geometry_state_2020": AssetIn(key=["geometry", "state", "2020"]),
    },
    io_manager_key="gpkg_manager",
)
def states_2020(
    geometry_state_2020: gpd.GeoDataFrame, state_2020: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_state_2020.drop(columns=["NOMGEO", "CVEGEO"])
        .assign(CVE_ENT=lambda df: df.CVE_ENT.astype(int))
        .set_index("CVE_ENT")
        .sort_index()
        .join(state_2020)
    )
    return merged
