import pandas as pd

from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


@asset(name="1990", key_prefix="scince")
def census_1990_scince(path_resource: PathResource) -> pd.DataFrame:
    census_path = Path(path_resource.raw_path) / "census/SCINCE/1990"
    census_a = (
        pd.concat(
            [
                pd.read_csv(f, usecols=["CVEGEO", "0"])
                .set_index("CVEGEO")
                .rename(columns={"0": "POBTOT"})
                for f in census_path.glob("*.csv")
            ]
        )
        .sort_index()
        .astype(int)
    )
    return census_a


@asset(name="2000", key_prefix="scince")
def census_2000_scince(path_resource: PathResource) -> pd.DataFrame:
    census_path = Path(path_resource.raw_path) / "census/SCINCE/2000"
    census_a = (
        pd.concat(
            [
                pd.read_csv(f, low_memory=False, usecols=["CVEGEO", "Z1"])
                .set_index("CVEGEO")
                .rename(columns={"Z1": "POBTOT"})
                for f in census_path.glob("*.csv")
            ]
        )
        .sort_index()
        .astype(int)
    )
    return census_a
