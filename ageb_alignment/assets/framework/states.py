import geopandas as gpd
import pandas as pd

from ageb_alignment.types import GeometryTuple
from dagster import asset, AssetIn


@asset(
    name="2000",
    key_prefix=["framework", "states"],
    ins={"geometry_2000": AssetIn(key=["geometry", "2000"])},
    io_manager_key="gpkg_manager",
)
def states_2000(geometry_2000: GeometryTuple) -> gpd.GeoDataFrame:
    merged = (
        geometry_2000.ent.drop(columns="OID")
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
        "geometry_2010": AssetIn(key=["geometry", "2010"]),
    },
    io_manager_key="gpkg_manager",
)
def states_2010(
    geometry_2010: GeometryTuple, state_2010: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_2010.ent.drop(columns=["OID", "NOM_ENT"])
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
        "geometry_2020": AssetIn(key=["geometry", "2020"]),
    },
    io_manager_key="gpkg_manager",
)
def states_2020(
    geometry_2020: GeometryTuple, state_2020: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_2020.ent.drop(columns=["NOMGEO", "CVEGEO"])
        .assign(CVE_ENT=lambda df: df.CVE_ENT.astype(int))
        .set_index("CVE_ENT")
        .sort_index()
        .join(state_2020)
    )
    return merged
