import pandas as pd

from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


def scince_factory(year: int, pop_col_name: str):
    @asset(name="ageb", key_prefix=["census", str(year)], group_name=f"census_{year}", io_manager_key="csv_manager")
    def _asset(path_resource: PathResource) -> pd.DataFrame:
        census_path = Path(path_resource.raw_path) / f"census/SCINCE/{year}"
        census_a = (
            pd.concat(
                [
                    pd.read_csv(f, usecols=["CVEGEO", pop_col_name])
                    .set_index("CVEGEO")
                    .rename(columns={pop_col_name: "POBTOT"})
                    for f in census_path.glob("*.csv")
                ]
            )
            .sort_index()
            .astype(int)
        )
        return census_a

    return _asset


dassets = [scince_factory(year, col) for year, col in zip([1990, 2000], ["0", "Z1"])]