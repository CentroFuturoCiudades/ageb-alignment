from pathlib import Path

import geopandas as gpd

from ageb_alignment.defs.resources import PathResource
from dagster import asset


@asset(
    key=["geometry", "loc", "2010"],
    io_manager_key="gpkg_manager",
    group_name="geometry_loc",
)
def geometry_loc_2010(path_resource: PathResource) -> gpd.GeoDataFrame:
    fpath = (
        Path(path_resource.data_path)
        / "initial"
        / "geometry"
        / "2010"
        / "mglu2010v5_0"
        / "Localidades_urbanas_2010_5.shp"
    )
    return (
        gpd.read_file(fpath)
        .assign(
            CVE_ENT=lambda df: df["CVE_ENT"].astype(str).str.zfill(2),
            CVE_MUN=lambda df: df["CVE_MUN"].astype(str).str.zfill(3),
            CVE_LOC=lambda df: df["CVE_LOC"].astype(str).str.zfill(4),
            CVEGEO=lambda df: df["CVE_ENT"] + df["CVE_MUN"] + df["CVE_LOC"],
        )
        .drop(columns=["CVE_ENT", "CVE_MUN", "CVE_LOC"])
        .to_crs("EPSG:6372")[["CVEGEO", "geometry"]]
    )


@asset(
    key=["geometry", "loc", "2020"],
    io_manager_key="gpkg_manager",
    group_name="geometry_loc",
)
def geometry_loc_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    fpath = Path(path_resource.data_path) / "initial" / "geometry" / "2020" / "00l.shp"
    return (
        gpd.read_file(fpath)
        .assign(CVEGEO=lambda df: df["CVEGEO"].astype(str).str.zfill(9))
        .to_crs("EPSG:6372")[["CVEGEO", "geometry"]]
    )
