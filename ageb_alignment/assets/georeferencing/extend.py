import shapely

import geopandas as gpd
import pandas as pd

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource
from dagster import asset, AssetDep, AssetExecutionContext
from pathlib import Path


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
    deps=[
        AssetDep(["zone_agebs", "replaced", "2000"]),
        AssetDep(["zone_agebs", "replaced", "2010"]),
    ],
    partitions_def=zone_partitions,
)
def zones_extended_2000(
    context: AssetExecutionContext, path_resource: PathResource
) -> tuple:
    zone = context.partition_key

    fixed_path = Path(path_resource.out_path) / "zone_agebs_replaced"
    df_source = (extend_gdf(gpd.read_file(fixed_path / f"2000/{zone}.gpkg")),)
    df_target = (extend_gdf(gpd.read_file(fixed_path / f"2010/{zone}.gpkg")),)

    return df_source, df_target
