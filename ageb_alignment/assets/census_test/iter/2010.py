import pandas as pd

from ageb_alignment.assets.census_test.iter.common import (
    load_census_iter_2010_2020,
    iter_factory,
)
from ageb_alignment.resources import PathResource
from dagster import op
from pathlib import Path


@op
def load_census_iter_2010(path_resource: PathResource) -> pd.DataFrame:
    census_path = Path(path_resource.raw_path) / "census/ITER/ITER_NALDBF10.csv"
    return load_census_iter_2010_2020(census_path)


dasset = iter_factory(2010, load_census_iter_2010)
