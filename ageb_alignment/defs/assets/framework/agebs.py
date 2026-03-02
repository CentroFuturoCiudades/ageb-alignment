import geopandas as gpd

import dagster as dg
from ageb_alignment.defs.resources import PostGISResource


def agebs_factory(year: int) -> dg.AssetsDefinition:
    @dg.asset(
        key=["framework", "agebs", str(year)],
        io_manager_key="gpkg_manager",
        group_name="agebs",
    )
    def _asset(postgis_resource: PostGISResource) -> gpd.GeoDataFrame:
        conn = postgis_resource.get_connection()
        return gpd.read_postgis(f"census_{year}_ageb", con=conn, geom_col="geometry")

    return _asset

assets = [agebs_factory(year) for year in range(1990, 2021, 10)]