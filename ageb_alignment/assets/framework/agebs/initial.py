import geopandas as gpd
import pandas as pd

from ageb_alignment.resources import PathResource
from ageb_alignment.types import GeometryTuple
from dagster import asset, AssetIn
from pathlib import Path


@asset(
    name="1990",
    key_prefix="agebs_initial",
    ins={
        "ageb_1990": AssetIn(key=["1990", "ageb"]),
        "geometry_1990": AssetIn(key=["geometry", "1990"]),
    },
)
def agebs_1990_initial(
    path_resource: PathResource, geometry_1990: GeometryTuple, ageb_1990: pd.DataFrame
) -> gpd.GeoDataFrame:
    out_path = Path(path_resource.out_path) / "framework/agebs"
    out_path.mkdir(exist_ok=True, parents=True)

    merged = (
        geometry_1990.ageb.join(ageb_1990, how="left")
        .fillna(0)
        .assign(POBTOT=lambda df: df.POBTOT.astype(int))
        .explode()
        .dissolve(by="CVEGEO")
    )
    return merged


@asset(
    name="2000",
    key_prefix="agebs_initial",
    ins={
        "ageb_2000": AssetIn(key=["2000", "ageb"]),
        "geometry_2000": AssetIn(key=["geometry", "2000"]),
    },
)
def agebs_2000_initial(
    path_resource: PathResource, geometry_2000: GeometryTuple, ageb_2000: pd.DataFrame
) -> gpd.GeoDataFrame:
    out_path = Path(path_resource.out_path) / "framework/agebs"
    out_path.mkdir(exist_ok=True, parents=True)

    merged = geometry_2000.ageb.join(ageb_2000, how="left").sort_index()
    return merged


@asset(
    name="2010",
    key_prefix="agebs_initial",
    ins={
        "ageb_2010": AssetIn(key=["2010", "ageb"]),
        "geometry_2010": AssetIn(key=["geometry", "2010"]),
    },
)
def agebs_2010_initial(
    path_resource: PathResource, geometry_2010: GeometryTuple, ageb_2010: pd.DataFrame
) -> gpd.GeoDataFrame:
    out_path = Path(path_resource.out_path) / "framework/agebs"
    out_path.mkdir(exist_ok=True, parents=True)

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


@asset(
    name="2020",
    key_prefix="agebs_initial",
    ins={
        "ageb_2020": AssetIn(key=["2020", "ageb"]),
        "geometry_2020": AssetIn(key=["geometry", "2020"]),
    },
)
def agebs_2020_initial(
    path_resource: PathResource, geometry_2020: GeometryTuple, ageb_2020: pd.DataFrame
) -> gpd.GeoDataFrame:
    out_path = Path(path_resource.out_path) / "framework/agebs"
    out_path.mkdir(exist_ok=True, parents=True)

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
