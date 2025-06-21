import geopandas as gpd
import pandas as pd

from ageb_alignment.assets.framework.agebs.common import framework_agebs_factory
from dagster import op


@op
def merge_agebs_2000(
    geometry_ageb_2000: gpd.GeoDataFrame, ageb_2000: pd.DataFrame
) -> gpd.GeoDataFrame:
    geometry_ageb_2000 = geometry_ageb_2000.set_index("CVEGEO")
    ageb_2000 = ageb_2000.assign(CVEGEO=lambda df: df["CVEGEO"].astype(str).str.zfill(13)).set_index("CVEGEO")

    merged = (
        geometry_ageb_2000
        .join(ageb_2000, how="left")
        .assign(POBTOT=lambda df: df["POBTOT"].fillna(0).astype(int))
        .sort_index()
    )
    return merged


framework_agebs_2000 = framework_agebs_factory(2000, merge_agebs_2000)
