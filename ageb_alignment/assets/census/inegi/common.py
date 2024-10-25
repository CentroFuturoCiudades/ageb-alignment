import pandas as pd

from ageb_alignment.resources import PathResource
from dagster import asset, AssetsDefinition
from pathlib import Path


def load_census_inegi(census_path: Path) -> pd.DataFrame:
    census = (
        pd.concat(
            [
                pd.read_csv(
                    f,
                    low_memory=False,
                    na_values=["*", "N/D"],
                    usecols=lambda x: x.upper()
                    in [
                        "ENTIDAD",
                        "NOM_ENT",
                        "MUN",
                        "NOM_MUN",
                        "LOC",
                        "AGEB",
                        "MZA",
                        "POBTOT",
                    ],
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


def inegi_factory(year: int) -> AssetsDefinition:
    @asset(name="ageb", key_prefix=str(year), group_name=f"census_{year}")
    def _asset(path_resource: PathResource) -> pd.DataFrame:
        fpath = Path(path_resource.raw_path) / f"census/INEGI/{year}"
        return load_census_inegi(fpath)

    return _asset
