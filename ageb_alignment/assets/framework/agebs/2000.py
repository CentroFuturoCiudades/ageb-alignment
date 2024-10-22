import geopandas as gpd
import pandas as pd

from ageb_alignment.assets.framework.agebs.common import framework_agebs_factory
from ageb_alignment.types import GeometryTuple
from dagster import op


@op
def merge_agebs_2000(
    geometry_2000: GeometryTuple, ageb_2000: pd.DataFrame
) -> gpd.GeoDataFrame:
    merged = geometry_2000.ageb.join(ageb_2000, how="left").sort_index()
    return merged


framework_agebs_2000 = framework_agebs_factory(2000, merge_agebs_2000)
