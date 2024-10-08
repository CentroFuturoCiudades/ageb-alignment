import geopandas as gpd
import pandas as pd

from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


YEARS = (1990, 2000, 2010, 2020)


def filter_by_metropoli(
    df: gpd.GeoDataFrame, metropoli: pd.DataFrame
) -> gpd.GeoDataFrame:
    df = df.copy()
    df["stub"] = df["CVEGEO"].str[:5]
    df = df.merge(metropoli, how="inner", on="stub")
    df = df.drop(columns=["stub"])
    df["geometry"] = df.make_valid()
    return df


def filter_and_merge(agebs: dict, census: dict, metropoli: pd.DataFrame) -> dict:
    agebs_filtered = {}
    for year in YEARS:
        df = filter_by_metropoli(agebs[year], metropoli)
        df = df.merge(census[year], how="inner", on="CVEGEO")
        agebs_filtered[year] = df
    return agebs_filtered


@asset
def filter_census(
    path_resource: PathResource,
    load_agebs: dict,
    load_census: dict,
    load_metropoli: pd.DataFrame,
) -> None:
    out_path = Path(path_resource.out_path) / "census_filtered"
    out_path.mkdir(exist_ok=True, parents=True)

    res = filter_and_merge(load_agebs, load_census, load_metropoli)
    for year, df in res.items():
        for name, group_df in df.groupby("METROPOLI"):
            if name == "playa del carmen":
                continue

            out_dir = out_path / name
            out_dir.mkdir(exist_ok=True, parents=True)

            group_df = group_df.drop(columns=["METROPOLI"])
            group_df.to_file(out_dir / f"{year}.geojson")
