from pathlib import Path

import geopandas as gpd

from ageb_alignment.defs.resources import PathResource
from dagster import asset


@asset(
    key=["geometry", "mun", "2000"],
    io_manager_key="gpkg_manager",
    group_name="geometry_mun",
)
def geometry_mun_2000(path_resource: PathResource) -> gpd.GeoDataFrame:
    fpath = (
        Path(path_resource.data_path)
        / "initial"
        / "geometry"
        / "2000"
        / "mgm2000"
        / "Municipios_2000.shp"
    )
    return (
        gpd.read_file(fpath)
        .assign(CVEGEO=lambda df: df["CVEMUNI"].astype(str).str.zfill(5))
        .to_crs("EPSG:6372")[["CVEGEO", "geometry"]]
    )


@asset(
    key=["geometry", "mun", "2010"],
    io_manager_key="gpkg_manager",
    group_name="geometry_mun",
)
def geometry_mun_2010(path_resource: PathResource) -> gpd.GeoDataFrame:
    fpath = (
        Path(path_resource.data_path)
        / "initial"
        / "geometry"
        / "2010"
        / "mgm2010v5_0"
        / "Municipios_2010_5.shp"
    )
    return (
        gpd.read_file(fpath)
        .assign(
            CVE_ENT=lambda df: df["CVE_ENT"].astype(str).str.zfill(2),
            CVE_MUN=lambda df: df["CVE_MUN"].astype(str).str.zfill(3),
            CVEGEO=lambda df: df["CVE_ENT"] + df["CVE_MUN"],
        )
        .to_crs("EPSG:6372")[["CVEGEO", "geometry"]]
    )


@asset(
    key=["geometry", "mun", "2020"],
    io_manager_key="gpkg_manager",
    group_name="geometry_mun",
)
def geometry_mun_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    fpath = (
        Path(path_resource.data_path) / "initial" / "geometry" / "2020" / "00mun.shp"
    )
    return (
        gpd.read_file(fpath)
        .assign(CVEGEO=lambda df: df["CVEGEO"].astype(str).str.zfill(5))
        .to_crs("EPSG:6372")[["CVEGEO", "geometry"]]
    )
