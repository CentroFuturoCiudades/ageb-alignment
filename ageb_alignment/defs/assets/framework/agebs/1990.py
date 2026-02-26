import geopandas as gpd
import pandas as pd

from ageb_alignment.defs.assets.framework.agebs.common import framework_agebs_factory
from dagster import op


@op
def merge_agebs_1990(
    geometry_ageb_1990: gpd.GeoDataFrame,
    ageb_1990: pd.DataFrame,
) -> gpd.GeoDataFrame:
    geometry_ageb_1990 = geometry_ageb_1990.set_index("CVEGEO")
    ageb_1990 = ageb_1990.assign(
        CVEGEO=lambda df: df["CVEGEO"].astype(str).str.zfill(13),
    ).set_index("CVEGEO")

    return (
        geometry_ageb_1990.join(ageb_1990, how="left")
        .assign(POBTOT=lambda df: df.POBTOT.fillna(0).astype(int))
        .explode()
        .dissolve(by="CVEGEO")
    )


framework_agebs_1990 = framework_agebs_factory(1990, merge_agebs_1990)
