import pandas as pd

from dagster import graph_multi_asset, op, AssetOut
from pathlib import Path
from typing import Callable


@op
def get_census_state(census_iter: pd.DataFrame) -> pd.DataFrame:
    return (
        census_iter.query("CVE_MUN == 0 & CVE_LOC == 0")
        .drop(columns=["NOM_MUN", "CVE_LOC", "NOM_LOC", "CVE_MUN"])
        .set_index("CVE_ENT")
        .sort_index()
    )


@op
def get_census_mun(census_iter: pd.DataFrame) -> pd.DataFrame:
    return (
        census_iter.query("CVE_MUN != 0 & CVE_LOC == 0")
        .drop(columns=["CVE_LOC", "NOM_LOC"])
        .assign(CVEGEO=lambda df: df.CVE_ENT * 1000 + df.CVE_MUN)
        .set_index("CVEGEO")
        .sort_index()
        .drop(columns=["CVE_ENT", "CVE_MUN"])
    )


@op
def get_census_loc(census_iter: pd.DataFrame) -> pd.DataFrame:
    return (
        census_iter.query("CVE_MUN != 0 & CVE_LOC != 0")
        .assign(
            CVEGEO=lambda df: df.CVE_ENT * 10000000 + df.CVE_MUN * 10000 + df.CVE_LOC
        )
        .set_index("CVEGEO")
        .sort_index()
        .drop(columns=["CVE_ENT", "CVE_MUN", "CVE_LOC"])
    )


def load_census_iter_2010_2020(census_path: Path) -> pd.DataFrame:
    census = (
        pd.read_csv(
            census_path,
            low_memory=False,
            usecols=[
                "ENTIDAD",
                "NOM_ENT",
                "MUN",
                "NOM_MUN",
                "LOC",
                "NOM_LOC",
                "POBTOT",
            ],
        )
        .rename(columns={"ENTIDAD": "CVE_ENT", "MUN": "CVE_MUN", "LOC": "CVE_LOC"})
        .query("CVE_ENT != 0")  # remove national totals
    )
    return census


# pylint: disable=no-value-for-parameter
def iter_factory(year: int, loading_func: Callable):
    @graph_multi_asset(
        name=f"census_{year}",
        outs={
            name: AssetOut(key=[str(year), name], group_name="asdasd")
            for name in ("state", "mun", "loc")
        },
        group_name=f"census_{year}",
    )
    def _asset():
        census_iter = loading_func()
        return {
            "state": get_census_state(census_iter),
            "mun": get_census_mun(census_iter),
            "loc": get_census_loc(census_iter),
        }

    return _asset
