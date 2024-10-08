import geopandas as gpd

from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path

# def _load_agebs_2000_fabian(path):
#     df = gpd.read_file(path, engine="pyogrio")
#     df = df[["CVE_AGEB", "Z1", "geometry"]]
#     df = df.rename(columns={"CVE_AGEB": "CVEGEO", "Z1": "POP"})
#     df["CVEGEO"] = df["CVEGEO"].str.replace("-", "")
#     return df


@asset
def load_agebs_1990(path_resource: PathResource) -> gpd.GeoDataFrame:
    path = Path(path_resource.raw_path) / "agebs/1990/AGEB_s_90_aj.shp"
    df = gpd.read_file(path, engine="pyogrio")
    df["CVEGEO"] = (
        df["CVE_ENT"].str.rjust(2, "0")
        + df["CVE_MUN"].str.rjust(3, "0")
        + df["CVE_LOC"].str.rjust(4, "0")
        + df["CVE_AGEB"].str.rjust(4, "0")
    )
    df = df[["CVEGEO", "geometry"]]
    df = df.to_crs("EPSG:6372")
    df["geometry"] = df.make_valid()
    return df


@asset
def load_agebs_2000(path_resource: PathResource) -> gpd.GeoDataFrame:
    path = Path(path_resource.raw_path) / "agebs/2000/agebs_urb_2000.shp"
    df = gpd.read_file(path, engine="pyogrio")

    df = df[["CLVAGB", "geometry"]]
    df = df.rename(columns={"CLVAGB": "CVEGEO"})

    df["CVEGEO"] = df["CVEGEO"].str.replace("-", "")

    df = df.to_crs("EPSG:6372")
    df["geometry"] = df.make_valid()
    return df


@asset
def load_agebs_2010(path_resource: PathResource) -> gpd.GeoDataFrame:
    path = Path(path_resource.raw_path) / "agebs/2010/agebs.shp"
    df = gpd.read_file(path, engine="pyogrio")
    df = df[["CVEGEO", "geometry"]]
    df = df.to_crs("EPSG:6372")
    df["geometry"] = df.make_valid()
    return df


@asset
def load_agebs_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    path = Path(path_resource.raw_path) / "agebs/2020/00a.shp"
    df = gpd.read_file(path, engine="pyogrio")
    df = df[df["Ambito"] == "Urbana"]
    df = df[["CVEGEO", "geometry"]]
    df = df.to_crs("EPSG:6372")
    df["geometry"] = df.make_valid()
    return df


@asset
def load_agebs(
    load_agebs_1990: gpd.GeoDataFrame,
    load_agebs_2000: gpd.GeoDataFrame,
    load_agebs_2010: gpd.GeoDataFrame,
    load_agebs_2020: gpd.GeoDataFrame,
) -> dict:
    return {
        1990: load_agebs_1990,
        2000: load_agebs_2000,
        2010: load_agebs_2010,
        2020: load_agebs_2020,
    }
