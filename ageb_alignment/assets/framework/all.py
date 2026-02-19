from typing import Literal

import geopandas as gpd
import pandas as pd

import dagster as dg


def framework_factory(
    geo_type: Literal["state", "mun", "loc"],
    year: int,
) -> dg.AssetsDefinition:
    @dg.asset(
        name=str(year),
        key_prefix=["framework", geo_type],
        ins={
            "census": dg.AssetIn(key=["census", str(year), geo_type]),
            "geometry": dg.AssetIn(key=["geometry", geo_type, str(year)]),
        },
        io_manager_key="gpkg_manager",
        group_name=f"framework_{geo_type}",
    )
    def _asset(
        census: pd.DataFrame,
        geometry: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        if geo_type == "state":
            census = census.assign(
                CVEGEO=lambda df: df["CVEGEO"].astype(str).str.zfill(2),
            )
        elif geo_type == "mun":
            census = census.assign(
                CVEGEO=lambda df: df["CVEGEO"].astype(str).str.zfill(5),
            )
        elif geo_type == "loc":
            census = census.assign(
                CVEGEO=lambda df: df["CVEGEO"].astype(str).str.zfill(9),
            )
        else:
            err = f"Invalid geo_type: {geo_type}. Must be one of 'state', 'mun', or 'loc'."
            raise ValueError(err)

        return geometry.merge(census, on="CVEGEO", how="left")

    return _asset


dassets = [
    framework_factory("mun", 2000),
    framework_factory("mun", 2010),
    framework_factory("mun", 2020),
    framework_factory("state", 2000),
    framework_factory("state", 2010),
    framework_factory("state", 2020),
]
