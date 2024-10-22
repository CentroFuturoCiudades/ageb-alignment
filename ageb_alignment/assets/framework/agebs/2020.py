import geopandas as gpd
import pandas as pd

from ageb_alignment.assets.framework.agebs.common import framework_agebs_factory
from ageb_alignment.types import GeometryTuple
from dagster import op


@op
def merge_agebs_2020(
    geometry_2020: GeometryTuple, ageb_2020: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_2020.ageb.drop(columns="Ambito")
        .assign(
            CVE_ENT=lambda df: df.CVE_ENT.astype(int),
            CVE_MUN=lambda df: df.CVE_MUN.astype(int),
            CVE_LOC=lambda df: df.CVE_LOC.astype(int),
        )
        .sort_index()
        .join(ageb_2020, how="left")
    )
    return merged


framework_agebs_2020 = framework_agebs_factory(2020, merge_agebs_2020)
