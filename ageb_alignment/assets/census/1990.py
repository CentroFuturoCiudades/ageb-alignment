import pandas as pd

from ageb_alignment.types import CensusTuple
from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


@asset
def census_1990_iter(path_resource: PathResource) -> pd.DataFrame:
    census_path = Path(path_resource.raw_path) / "census/ITER/ITER_NALTXT90.txt"
    census = (
        pd.read_csv(
            census_path,
            encoding="iso-8859-1",
            sep="\t",
            low_memory=False,
            usecols=[
                "entidad",
                "nom_ent",
                "mun",
                "nom_mun",
                "loc",
                "nom_loc",
                "p_total",
            ],
        )
        .rename(
            columns={
                "entidad": "CVE_ENT",
                "nom_ent": "NOM_ENT",
                "mun": "CVE_MUN",
                "nom_mun": "NOM_MUN",
                "loc": "CVE_LOC",
                "nom_loc": "NOM_LOC",
                "p_total": "POBTOT",
            }
        )
        .dropna()
        .assign(POBTOT=lambda df: df.POBTOT.astype(int))
        .query("CVE_ENT != 0")
    )
    return census


@asset
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


@asset(name="1990", key_prefix="census")
def census_1990(
    census_1990_iter: pd.DataFrame, census_1990_scince: pd.DataFrame
) -> CensusTuple:
    census_e = (
        census_1990_iter.query("CVE_MUN == 0 & CVE_LOC == 0")
        .drop(columns=["NOM_MUN", "CVE_LOC", "NOM_LOC", "CVE_MUN"])
        .set_index("CVE_ENT")
        .sort_index()
    )

    census_m = (
        census_1990_iter.query("CVE_MUN != 0 & CVE_LOC == 0")
        .drop(columns=["CVE_LOC", "NOM_LOC"])
        .assign(CVEGEO=lambda df: df.CVE_ENT * 1000 + df.CVE_MUN)
        .set_index("CVEGEO")
        .sort_index()
        .drop(columns=["CVE_ENT", "CVE_MUN"])
    )

    census_l = (
        census_1990_iter.query("CVE_MUN != 0 & CVE_LOC != 0")
        .assign(
            CVEGEO=lambda df: df.CVE_ENT * 10000000 + df.CVE_MUN * 10000 + df.CVE_LOC
        )
        .set_index("CVEGEO")
        .sort_index()
        .drop(columns=["CVE_ENT", "CVE_MUN", "CVE_LOC"])
    )

    return CensusTuple(
        ent=census_e, mun=census_m, loc=census_l, ageb=census_1990_scince
    )
