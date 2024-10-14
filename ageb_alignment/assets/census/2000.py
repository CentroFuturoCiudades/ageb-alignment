import pandas as pd

from ageb_alignment.types import CensusTuple
from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


@asset
def census_2000_iter(path_resource: PathResource) -> pd.DataFrame:
    census_path = Path(path_resource.raw_path) / "census/ITER/ITER_NALTXT00.txt"
    census = pd.read_csv(
        census_path,
        encoding="iso-8859-1",
        sep="\t",
        header=None,
        low_memory=False,
        usecols=[0, 1, 2, 3, 4, 5, 9],
        names=[
            "CVE_ENT",
            "NOM_ENT",
            "CVE_MUN",
            "NOM_MUN",
            "CVE_LOC",
            "NOM_LOC",
            "POBTOT",
        ],
    ).query("CVE_ENT != 0")
    return census


@asset
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


@asset(name="2000", key_prefix="census")
def census_2000(
    census_2000_iter: pd.DataFrame, census_2000_scince: pd.DataFrame
) -> CensusTuple:
    census_e = (
        census_2000_iter.query("CVE_MUN == 0 & CVE_LOC == 0")
        .drop(columns=["NOM_MUN", "CVE_LOC", "NOM_LOC", "CVE_MUN"])
        .set_index("CVE_ENT")
        .sort_index()
    )

    census_m = (
        census_2000_iter.query("CVE_MUN != 0 & CVE_LOC == 0")
        .drop(columns=["CVE_LOC", "NOM_LOC"])
        .assign(CVEGEO=lambda df: df.CVE_ENT * 1000 + df.CVE_MUN)
        .set_index("CVEGEO")
        .sort_index()
        .drop(columns=["CVE_ENT", "CVE_MUN"])
    )

    census_l = (
        census_2000_iter.query("CVE_MUN != 0 & CVE_LOC != 0")
        .assign(
            CVEGEO=lambda df: df.CVE_ENT * 10000000 + df.CVE_MUN * 10000 + df.CVE_LOC
        )
        .set_index("CVEGEO")
        .sort_index()
        .drop(columns=["CVE_ENT", "CVE_MUN", "CVE_LOC"])
    )

    return CensusTuple(
        ent=census_e, mun=census_m, loc=census_l, ageb=census_2000_scince
    )
