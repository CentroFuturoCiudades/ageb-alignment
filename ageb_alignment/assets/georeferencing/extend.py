import shapely

import geopandas as gpd
import pandas as pd

from ageb_alignment.partitions import zone_partitions
from dagster import asset, AssetIn


def get_outer_polygon(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    box = shapely.box(*gdf.total_bounds)
    box = shapely.buffer(box, 100)

    merged = shapely.unary_union(gdf["geometry"])
    cut = box.difference(merged)

    if isinstance(cut, shapely.geometry.Polygon):
        max_poly = cut
    else:
        max_area, max_poly = 0, None
        for poly in cut.geoms:
            area = poly.area
            if area > max_area:
                max_area = area
                max_poly = poly

    series = gpd.GeoDataFrame(
        ["OUT"], columns=["CVEGEO"], geometry=[max_poly], crs=gdf.crs
    )
    return series


def extend_gdf(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf.copy()
    gdf["geometry"] = gdf["geometry"].make_valid()
    outer_poly = get_outer_polygon(gdf)
    gdf = pd.concat([gdf, outer_poly], ignore_index=True)
    return gdf


@asset(
    name="2000",
    key_prefix="zones_extended",
    ins={
        "agebs_2000": AssetIn(key=["zone_agebs", "shaped", "2000"]),
        "agebs_2010": AssetIn(key=["zone_agebs", "shaped", "2010"]),
    },
    partitions_def=zone_partitions,
)
def zones_extended_2000(
    agebs_2000: gpd.GeoDataFrame, agebs_2010: gpd.GeoDataFrame
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    df_source = extend_gdf(agebs_2000)
    df_target = extend_gdf(agebs_2010)

    return df_source, df_target
