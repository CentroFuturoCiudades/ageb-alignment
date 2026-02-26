from pathlib import Path

import pandas as pd

import dagster as dg
from ageb_alignment.defs.resources import PathResource


def scince_factory(year: int, pop_col_name: str, extra_col: str) -> dg.AssetsDefinition:
    @dg.asset(
        key=["census", str(year), "ageb"],
        group_name=f"census_{year}",
        io_manager_key="csv_manager",
    )
    def _asset(path_resource: PathResource) -> pd.DataFrame:
        census_path = (
            Path(path_resource.data_path) / "initial" / "census" / "SCINCE" / str(year)
        )
        return (
            pd.concat(
                [
                    pd.read_csv(f, usecols=["CVEGEO", pop_col_name, extra_col])
                    .set_index("CVEGEO")
                    .rename(columns={pop_col_name: "POBTOT", extra_col: "P_12YMAS"})
                    for f in census_path.glob("*.csv")
                ],
            )
            .sort_index()
            .assign(
                POBTOT=lambda df: pd.to_numeric(df["POBTOT"], errors="coerce"),
                P_12YMAS=lambda df: pd.to_numeric(df["P_12YMAS"], errors="coerce"),
            )
        )

    return _asset


dassets = [
    scince_factory(year, col, extra_col)
    for year, col, extra_col in zip(
        [1990, 2000],
        ["0", "Z1"],
        ["4", "Z19"],
        strict=True,
    )
]
