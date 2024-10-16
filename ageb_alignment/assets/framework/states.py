import geopandas as gpd

from ageb_alignment.types import CensusTuple, GeometryTuple
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
        "census_2010": AssetIn(key=["census", "2010"]),
        "geometry_2010": AssetIn(key=["geometry", "2010"]),
    },
    io_manager_key="gpkg_manager",
)
def states_2010(
    geometry_2010: GeometryTuple, census_2010: CensusTuple
) -> gpd.GeoDataFrame:
    merged = (
        geometry_2010.ent.drop(columns=["OID", "NOM_ENT"])
        .assign(CVE_ENT=lambda df: df.CVE_ENT.astype(int))
        .set_index("CVE_ENT")
        .sort_index()
        .join(census_2010.ent)
    )
    return merged


@asset(
    name="2020",
    key_prefix=["framework", "states"],
    ins={
        "census_2020": AssetIn(key=["census", "2020"]),
        "geometry_2020": AssetIn(key=["geometry", "2020"]),
    },
    io_manager_key="gpkg_manager",
)
def states_2020(
    geometry_2020: GeometryTuple, census_2020: CensusTuple
) -> gpd.GeoDataFrame:
    merged = (
        geometry_2020.ent.drop(columns=["NOMGEO", "CVEGEO"])
        .assign(CVE_ENT=lambda df: df.CVE_ENT.astype(int))
        .set_index("CVE_ENT")
        .sort_index()
        .join(census_2020.ent)
    )
    return merged
