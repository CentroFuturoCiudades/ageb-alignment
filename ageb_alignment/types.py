import geopandas as gpd
import pandas as pd

from typing import NamedTuple, Optional


class GeometryTuple(NamedTuple):
    ent: Optional[gpd.GeoDataFrame] = None
    mun: Optional[gpd.GeoDataFrame] = None
    ageb: Optional[gpd.GeoDataFrame] = None
