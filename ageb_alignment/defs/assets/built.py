from pathlib import Path

import geopandas as gpd
import rasterio as rio
import rasterio.mask as rio_mask

import dagster as dg
from ageb_alignment.defs.partitions import zone_partitions
from ageb_alignment.defs.resources import PathResource


def built_up_area_factory(year: int) -> dg.AssetsDefinition:
    @dg.asset(
        key=["built_area", str(year)],
        ins={"polygons": dg.AssetIn(key=["reprojected", "base", str(year)])},
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
        group_name="built_area",
    )
    def _asset(
        path_resource: PathResource,
        polygons: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        built_path = Path(path_resource.ghsl_path) / f"BUILT_100/{year}.tif"

        areas = []
        with rio.open(built_path, nodata=0) as ds:
            polygons = polygons.to_crs(str(ds.crs))
            for geom in polygons["geometry"]:
                masked, _ = rio_mask.mask(ds, [geom], crop=True, nodata=0)
                areas.append(masked.sum())

        out = polygons[["geometry"]].copy()
        out["area"] = areas
        return out

    return _asset


area_assets = [built_up_area_factory(year) for year in (1990, 2000, 2010, 2020)]
