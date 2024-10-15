import pandas as pd

from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


def load_census_iter(census_path: Path) -> pd.DataFrame:
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


@asset(name="1990", key_prefix="iter")
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


@asset(name="2000", key_prefix="iter")
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


@asset(name="2010", key_prefix="iter")
def census_2010_iter(path_resource: PathResource) -> pd.DataFrame:
    census_path = Path(path_resource.raw_path) / "census/ITER/ITER_NALDBF10.csv"
    return load_census_iter(census_path)


@asset(name="2020", key_prefix="iter")
def census_2020_iter(path_resource: PathResource) -> pd.DataFrame:
    census_path = Path(path_resource.raw_path) / "census/ITER/ITER_NALCSV20.csv"
    return load_census_iter(census_path)
