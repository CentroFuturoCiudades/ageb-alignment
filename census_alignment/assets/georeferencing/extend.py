import shapely

import geopandas as gpd
import pandas as pd

from census_alignment.resources import PathResource
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

    series = gpd.GeoDataFrame(["OUT"], columns=["CVEGEO"], geometry=[max_poly], crs=gdf.crs)
    return series


def extend_gdf(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf.copy()
    gdf["geometry"] = gdf["geometry"].make_valid()
    outer_poly = get_outer_polygon(gdf)
    gdf = pd.concat([gdf, outer_poly], ignore_index=True)
    return gdf
    

@asset(deps=["filter_census"])
def extend_census_2000(path_resource: PathResource) -> dict:
    filtered_path = Path(path_resource.out_path) / "census_filtered"
    out_dict = {}
    for path in filtered_path.glob("*"):
        if not path.is_dir():
            continue

        city = path.stem.casefold()
        out_dict[city] = [
            extend_gdf(gpd.read_file(path / "2000.gpkg", engine="pyogrio")),
            extend_gdf(gpd.read_file(path / "2010.gpkg", engine="pyogrio"))
        ]
                    
    return out_dict
    

@asset(deps=["filter_census"])
def extend_census_1990(context: AssetExecutionContext, path_resource: PathResource) -> dict:
    filtered_path = Path(path_resource.out_path) / "census_filtered"
    georeferenced_path = Path(path_resource.out_path) / "georeferenced_2000"
    
    out_dict = {}
    for path in filtered_path.glob("*"):
        if not path.is_dir():
            continue
        
        city = path.stem.casefold()
        source_path = filtered_path / f"{city}/1990.gpkg"
        target_path = georeferenced_path / f"{city}.gpkg"

        if not target_path.exists():
            context.log.warning(f"Georeferenced data for {city} not found. Skipping.")
            continue

        out_dict[city] = [
            extend_gdf(gpd.read_file(source_path, engine="pyogrio")),
            extend_gdf(gpd.read_file(target_path, engine="pyogrio"))
        ]

    return out_dict