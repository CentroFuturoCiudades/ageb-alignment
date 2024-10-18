import pandas as pd

from ageb_alignment.assets.census_test.iter.common import iter_factory
from ageb_alignment.resources import PathResource
from dagster import op
from pathlib import Path


@op
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
            }
        )
        .dropna()
        .assign(POBTOT=lambda df: df.POBTOT.astype(int))
        .query("CVE_ENT != 0")
    )
    return census


dasset = iter_factory(1990, load_census_iter_1990)
