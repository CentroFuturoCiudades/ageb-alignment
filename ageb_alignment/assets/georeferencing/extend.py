import shapely

import geopandas as gpd
import pandas as pd

from ageb_alignment.resources import PathResource
from dagster import asset, AssetExecutionContext
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


@asset(deps=["zone_agebs_fixed_2000", "zone_agebs_fixed_2010"])
def zones_extended_2000(path_resource: PathResource) -> dict:
    fixed_path = Path(path_resource.out_path) / "zone_agebs_fixed"

    out_dict = {}
    temp_path = fixed_path / "1990"
    for path in temp_path.glob("*.gpkg"):
        zone = path.stem.casefold()

        out_dict[zone] = [
            extend_gdf(gpd.read_file(fixed_path / f"2000/{zone}.gpkg")),
            extend_gdf(gpd.read_file(fixed_path / f"2010/{zone}.gpkg")),
        ]

    return out_dict


# @asset(deps=["clean_census"])
# def extend_census_1990(
#     context: AssetExecutionContext, path_resource: PathResource
# ) -> dict:
#     fixed_path = Path(path_resource.out_path) / "census_fixed"
#     georeferenced_path = Path(path_resource.out_path) / "georeferenced_2000"

#     out_dict = {}
#     for path in fixed_path.glob("*"):
#         if not path.is_dir():
#             continue

#         city = path.stem.casefold()
#         source_path = fixed_path / f"{city}/1990.geojson"
#         target_path = georeferenced_path / f"{city}.geojson"

#         if not target_path.exists():
#             context.log.warning(f"Georeferenced data for {city} not found. Skipping.")
#             continue

#         out_dict[city] = [
#             extend_gdf(gpd.read_file(source_path, engine="pyogrio")),
#             extend_gdf(gpd.read_file(target_path, engine="pyogrio")),
#         ]

#     return out_dict
