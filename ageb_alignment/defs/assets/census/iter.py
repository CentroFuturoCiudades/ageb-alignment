from collections.abc import Callable
from pathlib import Path

import pandas as pd

import dagster as dg
from ageb_alignment.defs.resources import PathResource

FIELD_NAMES = ("state", "mun", "loc")


@dg.op
def get_census_state(census_iter: pd.DataFrame) -> pd.DataFrame:
    return (
        census_iter.query("CVE_MUN == 0 & CVE_LOC == 0")
        .drop(columns=["NOM_MUN", "CVE_LOC", "NOM_LOC", "CVE_MUN"])
        .assign(CVEGEO=lambda df: df["CVE_ENT"].astype(str).str.zfill(2))
        .set_index("CVEGEO")
        .sort_index()[["POBTOT"]]
    )


@dg.op
def get_census_mun(census_iter: pd.DataFrame) -> pd.DataFrame:
    return (
        census_iter.query("CVE_MUN != 0 & CVE_LOC == 0")
        .drop(columns=["CVE_LOC", "NOM_LOC"])
        .assign(
            CVE_ENT=lambda df: df["CVE_ENT"].astype(str).str.zfill(2),
            CVE_MUN=lambda df: df["CVE_MUN"].astype(str).str.zfill(3),
            CVEGEO=lambda df: df["CVE_ENT"] + df["CVE_MUN"],
        )
        .set_index("CVEGEO")
        .sort_index()[["POBTOT"]]
    )


@dg.op
def get_census_loc(census_iter: pd.DataFrame) -> pd.DataFrame:
    return (
        census_iter.query("CVE_MUN != 0 & CVE_LOC != 0")
        .assign(
            CVE_ENT=lambda df: df["CVE_ENT"].astype(str).str.zfill(2),
            CVE_MUN=lambda df: df["CVE_MUN"].astype(str).str.zfill(3),
            CVE_LOC=lambda df: df["CVE_LOC"].astype(str).str.zfill(4),
            CVEGEO=lambda df: df["CVE_ENT"] + df["CVE_MUN"] + df["CVE_LOC"],
        )
        .set_index("CVEGEO")
        .sort_index()[["POBTOT"]]
    )


@dg.op(
    out={
        name: dg.Out(is_required=False, io_manager_key="csv_manager")
        for name in FIELD_NAMES
    },
)
def census_dispatcher(context: dg.OpExecutionContext, census_iter: pd.DataFrame):
    if "state" in context.selected_output_names:
        yield dg.Output(get_census_state(census_iter), output_name="state")
    if "mun" in context.selected_output_names:
        yield dg.Output(get_census_mun(census_iter), output_name="mun")
    if "loc" in context.selected_output_names:
        yield dg.Output(get_census_loc(census_iter), output_name="loc")


@dg.op
def load_census_iter_1990(path_resource: PathResource) -> pd.DataFrame:
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
            },
        )
        .dropna()
        .assign(POBTOT=lambda df: df.POBTOT.astype(int))
        .query("CVE_ENT != 0")
    )
    return census


@dg.op
def load_census_iter_2000(path_resource: PathResource) -> pd.DataFrame:
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
        .query("CVE_ENT != 0")
    )
    return census


@dg.op
def load_census_iter_2010(path_resource: PathResource) -> pd.DataFrame:
    census_path = Path(path_resource.raw_path) / "census/ITER/ITER_NALDBF10.csv"
    return load_census_iter_2010_2020(census_path)


@dg.op
def load_census_iter_2020(path_resource: PathResource) -> pd.DataFrame:
    census_path = Path(path_resource.raw_path) / "census/ITER/ITER_NALCSV20.csv"
    return load_census_iter_2010_2020(census_path)


def iter_factory(year: int, loading_func: Callable):
    @dg.graph_multi_asset(
        name=f"census_{year}",
        outs={
            name: dg.AssetOut(key=["census", str(year), name]) for name in FIELD_NAMES
        },
        group_name=f"census_{year}",
        can_subset=True,
    )
    def _asset():
        state, mun, loc = census_dispatcher(loading_func())
        return {"state": state, "mun": mun, "loc": loc}

    return _asset


dassets = [
    iter_factory(year, f)
    for year, f in zip(
        [1990, 2000, 2010, 2020],
        [
            load_census_iter_1990,
            load_census_iter_2000,
            load_census_iter_2010,
            load_census_iter_2020,
        ],
    )
]
