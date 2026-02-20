import geopandas as gpd
import pandas as pd

import dagster as dg


@dg.asset(
    key=["framework", "blocks", "2020"],
    ins={
        "census": dg.AssetIn(key=["census", "2020", "blocks"]),
        "geometry": dg.AssetIn(key=["geometry", "blocks", "2020"]),
    },
    io_manager_key="gpkg_manager",
    group_name="framework_blocks",
)
def framework_blocks_2020(
    census: pd.DataFrame,
    geometry: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    return (
        geometry.set_index("CVEGEO")
        .assign(
            CVE_ENT=lambda df: df.CVE_ENT.astype(int),
            CVE_MUN=lambda df: df.CVE_MUN.astype(int),
            CVE_LOC=lambda df: df.CVE_LOC.astype(int),
        )
        .sort_index()
        .join(
            census.set_index("CVEGEO"),
            how="left",
        )
    )
