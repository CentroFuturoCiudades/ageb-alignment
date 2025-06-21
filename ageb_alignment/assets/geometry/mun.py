import geopandas as gpd

from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


@asset(name="2000", key_prefix=["geometry", "mun"], io_manager_key="gpkg_manager")
def geometry_mun_2000(path_resource: PathResource) -> gpd.GeoDataFrame:
    fpath = (
        Path(path_resource.raw_path) / "geometry" / "2000" / "mgm2000" / "Municipios_2000.shp"
    )
    mg_2000_m = (
        gpd.read_file(fpath)
        .assign(CVEGEO=lambda df: df["CVEMUNI"].astype(str).str.zfill(5))
        .to_crs("EPSG:6372")
        [["CVEGEO", "geometry"]]
    )
    return mg_2000_m


@asset(name="2010", key_prefix=["geometry", "mun"], io_manager_key="gpkg_manager")
def geometry_mun_2010(path_resource: PathResource) -> gpd.GeoDataFrame:
    fpath = Path(path_resource.raw_path) / "geometry" / "2010" /"mgm2010v5_0" / "Municipios_2010_5.shp"
    return (
        gpd.read_file(fpath)
        .assign(
            CVE_ENT=lambda df: df["CVE_ENT"].astype(str).str.zfill(2),
            CVE_MUN=lambda df: df["CVE_MUN"].astype(str).str.zfill(3),
            CVEGEO=lambda df: df["CVE_ENT"] + df["CVE_MUN"],
        )
        .to_crs("EPSG:6372")
        [["CVEGEO", "geometry"]]
    )


@asset(name="2020", key_prefix=["geometry", "mun"], io_manager_key="gpkg_manager")
def geometry_mun_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    fpath = Path(path_resource.raw_path) / "geometry" / "2020" /"00mun.shp"
    return (
        gpd.read_file(fpath)
        .assign(CVEGEO=lambda df: df["CVEGEO"].astype(str).str.zfill(5))
        .to_crs("EPSG:6372")
        [["CVEGEO", "geometry"]]
    )
