import geopandas as gpd
import pandas as pd

from typing import NamedTuple, Optional


class GeometryTuple(NamedTuple):
    ent: Optional[gpd.GeoDataFrame] = None
    mun: Optional[gpd.GeoDataFrame] = None
    ageb: Optional[gpd.GeoDataFrame] = None


class CensusTuple(NamedTuple):
    ent: pd.DataFrame
    mun: pd.DataFrame
    loc: pd.DataFrame
    ageb: pd.DataFrame
