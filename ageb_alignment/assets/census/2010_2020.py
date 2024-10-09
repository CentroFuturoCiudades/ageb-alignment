import pandas as pd

from ageb_alignment.types import CensusTuple
from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


def _load_census_iter(census_path: Path) -> pd.DataFrame:
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


def _load_census_inegi(census_path: Path, *, to_low: bool) -> pd.DataFrame:
    usecols = ["ENTIDAD", "NOM_ENT", "MUN", "NOM_MUN", "LOC", "AGEB", "MZA", "POBTOT"]

    if to_low:
        usecols = [c.lower() for c in usecols]

    census = (
        pd.concat(
            [
                pd.read_csv(
                    f,
                    low_memory=False,
                    na_values=["*", "N/D"],
                    usecols=usecols,
                )
                .pipe(lambda df: df.set_axis(df.columns.str.upper(), axis=1))
                .query("MZA == 0 & AGEB != '0000' & AGEB != '0'")
                .drop(columns=["MZA"])
                for f in census_path.glob("*.csv")
            ]
        )
        .rename(
            columns={
                "ENTIDAD": "CVE_ENT",
                "MUN": "CVE_MUN",
                "LOC": "CVE_LOC",
                "AGEB": "CVE_AGEB",
            }
        )
        .assign(
            CVEGEO=lambda df: df.CVE_ENT.astype(str).str.pad(2, "left", "0")
            + df.CVE_MUN.astype(str).str.pad(3, "left", "0")
            + df.CVE_LOC.astype(str).str.pad(4, "left", "0")
            + df.CVE_AGEB.str.pad(4, "left", "0")
        )
        .set_index("CVEGEO")
        .sort_index()
        .drop(columns=["CVE_ENT", "CVE_MUN", "CVE_LOC", "CVE_AGEB"])
    )
    return census


@asset
def census_2010_iter(path_resource: PathResource) -> pd.DataFrame:
    census_path = Path(path_resource.raw_path) / "census/ITER/ITER_NALDBF10.csv"
    return _load_census_iter(census_path)


@asset
def census_2020_iter(path_resource: PathResource) -> pd.DataFrame:
    census_path = Path(path_resource.raw_path) / "census/ITER/ITER_NALCSV20.csv"
    return _load_census_iter(census_path)


@asset
def census_2010_inegi(path_resource: PathResource) -> pd.DataFrame:
    census_path = Path(path_resource.raw_path) / "census/INEGI/2010"
    return _load_census_inegi(census_path, to_low=True)


@asset
def census_2020_inegi(path_resource: PathResource) -> pd.DataFrame:
    census_path = Path(path_resource.raw_path) / "census/INEGI/2020"
    return _load_census_inegi(census_path, to_low=False)


def _merge_census(
    census_iter: pd.DataFrame, census_inegi: pd.DataFrame
) -> pd.DataFrame:
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
            CVEGEO=lambda df: df.CVE_ENT * 10000000 + df.CVE_MUN * 10000 + df.CVE_LOC
        )
        .set_index("CVEGEO")
        .sort_index()
        .drop(columns=["CVE_ENT", "CVE_MUN", "CVE_LOC"])
    )

    return CensusTuple(ent=census_e, mun=census_m, loc=census_l, ageb=census_inegi)


@asset
def census_2010(
    census_2010_iter: pd.DataFrame, census_2010_inegi: pd.DataFrame
) -> CensusTuple:
    return _merge_census(census_2010_iter, census_2010_inegi)


@asset
def census_2020(
    census_2020_iter: pd.DataFrame, census_2020_inegi: pd.DataFrame
) -> CensusTuple:
    return _merge_census(census_2020_iter, census_2020_inegi)
