import pandas as pd

from ageb_alignment.types import CensusTuple
from dagster import asset, AssetIn


def census_1990_2000_factory(year: int) -> asset:
    @asset(
        name=str(year),
        key_prefix="census",
        ins={
            "census_iter": AssetIn(key=["iter", str(year)]),
            "census_scince": AssetIn(key=["scince", str(year)]),
        },
    )
    def _asset(census_iter: pd.DataFrame, census_scince: pd.DataFrame) -> CensusTuple:
        census_e = (
            census_iter.query("CVE_MUN == 0 & CVE_LOC == 0")
            .drop(columns=["NOM_MUN", "CVE_LOC", "NOM_LOC", "CVE_MUN"])
            .set_index("CVE_ENT")
            .sort_index()
        )

        census_m = (
            census_iter.query("CVE_MUN != 0 & CVE_LOC == 0")
            .drop(columns=["CVE_LOC", "NOM_LOC"])
            .assign(CVEGEO=lambda df: df.CVE_ENT * 1000 + df.CVE_MUN)
            .set_index("CVEGEO")
            .sort_index()
            .drop(columns=["CVE_ENT", "CVE_MUN"])
        )

        census_l = (
            census_iter.query("CVE_MUN != 0 & CVE_LOC != 0")
            .assign(
                CVEGEO=lambda df: df.CVE_ENT * 10000000
                + df.CVE_MUN * 10000
                + df.CVE_LOC
            )
            .set_index("CVEGEO")
            .sort_index()
            .drop(columns=["CVE_ENT", "CVE_MUN", "CVE_LOC"])
        )

        return CensusTuple(ent=census_e, mun=census_m, loc=census_l, ageb=census_scince)

    return _asset


def census_2010_2020_factory(year: int) -> asset:
    @asset(
        name=str(year),
        key_prefix="census",
        ins={
            "census_iter": AssetIn(key=["iter", str(year)]),
            "census_inegi": AssetIn(key=["inegi", str(year)]),
        },
    )
    def _asset(census_iter: pd.DataFrame, census_inegi: pd.DataFrame) -> CensusTuple:
        census_e = (
            census_iter.query("CVE_MUN == 0 & CVE_LOC == 0")
            .drop(columns=["NOM_MUN", "CVE_LOC", "NOM_LOC", "CVE_MUN"])
            .set_index("CVE_ENT")
            .sort_index()
        )

        census_m = (
            census_iter.query("CVE_MUN != 0 & CVE_LOC == 0")
            .drop(columns=["CVE_LOC", "NOM_LOC"])
            .assign(CVEGEO=lambda df: df.CVE_ENT * 1000 + df.CVE_MUN)
            .set_index("CVEGEO")
            .sort_index()
            .drop(columns=["CVE_ENT", "CVE_MUN"])
        )

        census_l = (
            census_iter.query("CVE_MUN != 0 & CVE_LOC != 0")
            .assign(
                CVEGEO=lambda df: df.CVE_ENT * 10000000
                + df.CVE_MUN * 10000
                + df.CVE_LOC
            )
            .set_index("CVEGEO")
            .sort_index()
            .drop(columns=["CVE_ENT", "CVE_MUN", "CVE_LOC"])
        )

        return CensusTuple(ent=census_e, mun=census_m, loc=census_l, ageb=census_inegi)

    return _asset


census_assets = [census_1990_2000_factory(year) for year in (1990, 2000)] + [
    census_2010_2020_factory(year) for year in (2010, 2020)
]
