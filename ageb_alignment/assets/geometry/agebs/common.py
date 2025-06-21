import dagster as dg
import geopandas as gpd
import numpy as np

from ageb_alignment.resources import AgebListResource


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


def fix_overlapped_op_factory(year: int) -> dg.OpDefinition:
    @dg.op(name=f"fix_overlapped_{year}", out=dg.Out(io_manager_key="gpkg_manager"))
    def _op(
        context: dg.OpExecutionContext,
        overlap_resource: AgebListResource,
        agebs: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        overlapped = getattr(overlap_resource, f"ageb_{year}")
        if overlapped is not None:
            context.log.info(f"Fixed overlaps for {year}.")
            agebs = fix_overlapped(agebs, overlapped)

        return agebs[["geometry"]]


    return _op
