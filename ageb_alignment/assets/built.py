import rasterio.mask

import geopandas as gpd
import rasterio as rio

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource
from dagster import asset, AssetIn
from pathlib import Path


def built_up_area_factory(year: int):
    @asset(
        name=str(year),
        key_prefix="built_area",
        ins={"polygons": AssetIn(key=["reprojected", "base", str(year)])},
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
    )
    def _asset(
        path_resource: PathResource, polygons: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        built_path = Path(path_resource.ghsl_path) / f"BUILT_100/{year}.tif"

        areas = []
        with rio.open(built_path, nodata=0) as ds:
            polygons = polygons.to_crs(str(ds.crs))
            for geom in polygons["geometry"]:
                masked, _ = rio.mask.mask(ds, [geom], crop=True, nodata=0)
                areas.append(masked.sum())

        out = polygons[["geometry"]].copy()
        out["area"] = areas
        return out

    return _asset


area_assets = [built_up_area_factory(year) for year in (1990, 2000, 2010, 2020)]
