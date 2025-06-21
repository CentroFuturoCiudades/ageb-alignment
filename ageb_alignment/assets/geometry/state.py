import geopandas as gpd

from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


def read_state(fpath: Path) -> gpd.GeoDataFrame:
    return (
        gpd.read_file(fpath)
        .assign(CVEGEO=lambda df: df["CVE_ENT"].astype(str).str.zfill(2))
        .to_crs("EPSG:6372")[["CVEGEO", "geometry"]]
    )


@asset(name="2000", key_prefix=["geometry", "state"], io_manager_key="gpkg_manager")
def geometry_state_2000(path_resource: PathResource) -> gpd.GeoDataFrame:
    fpath = (
        Path(path_resource.raw_path) / "geometry" / "2000" / "mge2000" / "Entidades_2000.shp"
    )
    return read_state(fpath)


@asset(name="2010", key_prefix=["geometry", "state"], io_manager_key="gpkg_manager")
def geometry_state_2010(path_resource: PathResource) -> gpd.GeoDataFrame:
    fpath = Path(path_resource.raw_path) / "geometry" / "2010" / "mge2010v5_0" / "Entidades_2010_5.shp"
    return read_state(fpath)


@asset(name="2020", key_prefix=["geometry", "state"], io_manager_key="gpkg_manager")
def geometry_state_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    fpath = Path(path_resource.raw_path) / "geometry" / "2020" / "00ent.shp"
    return read_state(fpath)
