import geopandas as gpd
import pandas as pd

from ageb_alignment.types import GeometryTuple
from dagster import op, AssetIn


@op
def prep_agebs_1990(
    geometry_1990: GeometryTuple, ageb_1990: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_1990.ageb.join(ageb_1990, how="left")
        .fillna(0)
        .assign(POBTOT=lambda df: df.POBTOT.astype(int))
        .explode()
        .dissolve(by="CVEGEO")
    )
    return merged


@op
def prep_agebs_2000(
    geometry_2000: GeometryTuple, ageb_2000: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = geometry_2000.ageb.join(ageb_2000, how="left").sort_index()
    return merged


@op
def prep_agebs_2010(
    geometry_2010: GeometryTuple, ageb_2010: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_2010.ageb.drop(
            columns=[
                "CODIGO",
                "GEOGRAFICO",
                "FECHAACT",
                "GEOMETRIA",
                "INSTITUCIO",
                "OID",
            ]
        )
        .assign(
            CVE_ENT=lambda df: df.index.str[0:2].astype(int),
            CVE_MUN=lambda df: df.index.str[2:5].astype(int),
            CVE_LOC=lambda df: df.index.str[5:9].astype(int),
            CVE_AGEB=lambda df: df.index.str[9:],
        )
        .sort_index()
        .join(ageb_2010, how="left")
        .assign(POBTOT=lambda df: df.POBTOT.fillna(0).astype(int))
    )
    return merged


@op
def prep_agebs_2020(
    geometry_2020: GeometryTuple, ageb_2020: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_2020.ageb.drop(columns="Ambito")
        .assign(
            CVE_ENT=lambda df: df.CVE_ENT.astype(int),
            CVE_MUN=lambda df: df.CVE_MUN.astype(int),
            CVE_LOC=lambda df: df.CVE_LOC.astype(int),
        )
        .sort_index()
        .join(ageb_2020, how="left")
    )
    return merged
