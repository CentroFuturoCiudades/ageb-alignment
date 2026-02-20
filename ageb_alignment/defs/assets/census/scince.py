from pathlib import Path

import pandas as pd

import dagster as dg
from ageb_alignment.defs.resources import PathResource


def scince_factory(year: int, pop_col_name: str) -> dg.AssetsDefinition:
    @dg.asset(
        key=["census", str(year), "ageb"],
        group_name=f"census_{year}",
        io_manager_key="csv_manager",
    )
    def _asset(path_resource: PathResource) -> pd.DataFrame:
        census_path = Path(path_resource.raw_path) / f"census/SCINCE/{year}"
        return (
            pd.concat(
                [
                    pd.read_csv(f, usecols=["CVEGEO", pop_col_name])
                    .set_index("CVEGEO")
                    .rename(columns={pop_col_name: "POBTOT"})
                    for f in census_path.glob("*.csv")
                ],
            )
            .sort_index()
            .astype(int)
        )

    return _asset


dassets = [
    scince_factory(year, col)
    for year, col in zip([1990, 2000], ["0", "Z1"], strict=True)
]
