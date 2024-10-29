import geopandas as gpd
import pandas as pd

from ageb_alignment.assets.framework.agebs.common import framework_agebs_factory
from dagster import op


@op
def merge_agebs_1990(
    geometry_ageb_1990: gpd.GeoDataFrame, ageb_1990: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = (
        geometry_ageb_1990.join(ageb_1990, how="left")
        .fillna(0)
        .assign(POBTOT=lambda df: df.POBTOT.astype(int))
        .explode()
        .dissolve(by="CVEGEO")
    )
    return merged


framework_agebs_1990 = framework_agebs_factory(1990, merge_agebs_1990)
