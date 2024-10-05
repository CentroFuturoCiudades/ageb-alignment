import shapely

import geopandas as gpd
import pandas as pd

from census_alignment.resources import PathResource
from dagster import asset, op
from pathlib import Path


@op
def get_outer_polygon(gdf):
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


@op
def extend_gdf(gdf):
    gdf = gdf.copy()
    gdf["geometry"] = gdf["geometry"].make_valid()
    outer_poly = get_outer_polygon(gdf)
    gdf = pd.concat([gdf, outer_poly], ignore_index=True)
    return gdf


@asset(deps=["filter_census"])
def extend_census(path_resource: PathResource) -> None:
    root_out_path = Path(path_resource.out_path)
    
    filtered_path = root_out_path / "census_filtered"
    
    out_path = root_out_path / "census_extended"
    out_path.mkdir(exist_ok=True, parents=True)

    for path in filtered_path.glob("*"):
        if not path.is_dir():
            continue

        city = path.stem.casefold()
        for year in (1990, 2000, 2010, 2020):
            fpath = path / f"{year}.gpkg"
            df = gpd.read_file(fpath, engine="pyogrio")
            df = extend_gdf(df)

            out_subdir = out_path / city
            out_subdir.mkdir(exist_ok=True, parents=True)
            df.to_file(out_subdir / f"{year}.gpkg")