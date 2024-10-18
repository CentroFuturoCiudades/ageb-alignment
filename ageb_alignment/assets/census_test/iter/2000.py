import pandas as pd

from ageb_alignment.assets.census_test.iter.common import iter_factory
from ageb_alignment.resources import PathResource
from dagster import op
from pathlib import Path


@op
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


dasset = iter_factory(2000, load_census_iter_2000)
