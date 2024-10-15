import geopandas as gpd
import numpy as np


def fix_overlapped(gdf: gpd.GeoDataFrame, cover_list: list) -> gpd.GeoDataFrame:
    """Fix geometries covering whole other geometries by removing the overlapping part."""
    gdf = gdf.copy()
    cover_list = np.array(cover_list)

    for source, target in cover_list:
        new_geoms = (
            gdf.loc[[source]]
            .geometry.difference(gdf.loc[[target]].geometry, align=False)
            .explode()
            .to_frame()
            .assign(AREA=lambda df: df.area)
            .sort_values("AREA", ascending=False)
            .groupby("CVEGEO")
            .first()
            .rename(columns={0: "geometry"})
            .set_geometry("geometry")
        )

        gdf.loc[[source], "geometry"] = new_geoms.loc[[source], "geometry"]

    return gdf
