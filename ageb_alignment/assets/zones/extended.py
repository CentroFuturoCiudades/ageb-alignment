import shapely

import geopandas as gpd
import pandas as pd

from ageb_alignment.partitions import zone_partitions
from dagster import asset, AssetIn, AssetsDefinition


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


def zones_extended_factory(year: int) -> AssetsDefinition:
    @asset(
        name=str(year),
        key_prefix=["zone_agebs", "extended"],
        ins={
            "agebs": AssetIn(key=["zone_agebs", "shaped", str(year)]),
        },
        io_manager_key="gpkg_manager",
        partitions_def=zone_partitions,
    )
    def _asset(agebs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        agebs = agebs.copy()
        agebs["geometry"] = agebs["geometry"].make_valid()
        outer_poly = get_outer_polygon(agebs)
        agebs = gpd.GeoDataFrame(pd.concat([agebs, outer_poly], ignore_index=True))
        return agebs

    return _asset


zones_extended = [zones_extended_factory(year) for year in (1990, 2000, 2010)]
