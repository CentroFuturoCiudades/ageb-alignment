import geopandas as gpd
import pandas as pd

from ageb_alignment.assets.framework.agebs.common import framework_agebs_factory
from ageb_alignment.types import GeometryTuple
from dagster import op


@op
def merge_agebs_2010(
    geometry_2010: GeometryTuple, ageb_2010: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_2010.ageb.drop(
            columns=[
                "CODIGO",
                "GEOGRAFICO",
                "FECHAACT",
                "GEOMETRIA",
                "INSTITUCIO",
                "OID",
            ]
        )
        .assign(
            CVE_ENT=lambda df: df.index.str[0:2].astype(int),
            CVE_MUN=lambda df: df.index.str[2:5].astype(int),
            CVE_LOC=lambda df: df.index.str[5:9].astype(int),
            CVE_AGEB=lambda df: df.index.str[9:],
        )
        .sort_index()
        .join(ageb_2010, how="left")
        .assign(POBTOT=lambda df: df.POBTOT.fillna(0).astype(int))
    )
    return merged


framework_agebs_2010 = framework_agebs_factory(2010, merge_agebs_2010)
