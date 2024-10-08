import pandas as pd

from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


# def _load_census_1990_fabian(path):
#     df = pd.read_csv(path)
#     df = df.dropna(subset=["ENT", "MUN", "LOC", "AGEB"])
#     df["MUN"] = df["MUN"].astype(int).astype(str)
#     df["LOC"] = df["LOC"].astype(int).astype(str)
#     df["CVEGEO"] = df["ENT"].str.rjust(2, "0") + df["MUN"].str.rjust(3, "0") + df["LOC"].str.rjust(4, "0") + df["AGEB"].str.rjust(4, "0")
#     df = df[["CVEGEO", "Población total"]]
#     df = df.rename(columns={"Población total": "POP"})
#     return df


@asset
def load_census_1990(path_resource: PathResource) -> pd.DataFrame:
    df_all = []

    dir_path = Path(path_resource.raw_path) / "census/1990"
    for path in dir_path.glob("*.csv"):
        df = pd.read_csv(path, index_col="CVEGEO")
        df = df[["0"]]
        df.columns = ["POP"]
        df_all.append(df)

    df_all = pd.concat(df_all)
    df_all = df_all.reset_index()
    return df_all


@asset
def load_census_2000(path_resource: PathResource) -> pd.DataFrame:
    dir_path = Path(path_resource.raw_path) / "census/2000"

    df_all = []
    for path in dir_path.glob("*.csv"):
        df = pd.read_csv(path, index_col=0)
        df.index.name = "CVEGEO"
        df = df[["Z1"]]
        df.columns = ["POP"]
        df_all.append(df)

    df_all = pd.concat(df_all)
    df_all = df_all.reset_index()
    return df_all


def _load_census_2010_2020(dir_path: Path) -> pd.DataFrame:
    df = []
    for subpath in (dir_path).glob("*.csv"):
        df.append(pd.read_csv(subpath))
    df = pd.concat(df)
    df.columns = [c.casefold() for c in df.columns]

    df = df[df["nom_loc"] == "Total AGEB urbana"]
    df = df.reset_index(drop=True)

    for col in ["entidad", "mun", "loc", "ageb"]:
        df[col] = df[col].astype(str)
    df["CVEGEO"] = (
        df["entidad"].str.rjust(2, "0")
        + df["mun"].str.rjust(3, "0")
        + df["loc"].str.rjust(4, "0")
        + df["ageb"].str.rjust(4, "0")
    )
    df = df[["CVEGEO", "pobtot"]]
    df = df.rename(columns={"pobtot": "POP"})
    return df


@asset
def load_census_2010(path_resource: PathResource) -> pd.DataFrame:
    dir_path = Path(path_resource.raw_path) / "census/2010"
    return _load_census_2010_2020(dir_path)


@asset
def load_census_2020(path_resource: PathResource) -> pd.DataFrame:
    dir_path = Path(path_resource.raw_path) / "census/2020"
    return _load_census_2010_2020(dir_path)


@asset
def load_census(
    load_census_1990: pd.DataFrame,
    load_census_2000: pd.DataFrame,
    load_census_2010: pd.DataFrame,
    load_census_2020: pd.DataFrame,
) -> dict:
    return {
        1990: load_census_1990,
        2000: load_census_2000,
        2010: load_census_2010,
        2020: load_census_2020,
    }
