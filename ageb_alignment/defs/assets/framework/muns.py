import geopandas as gpd

import dagster as dg
from ageb_alignment.defs.resources import PostGISResource


def muns_factory(year: int) -> dg.AssetsDefinition:
    @dg.asset(
        key=["framework", "mun", str(year)],
        io_manager_key="gpkg_manager",
        group_name="muns",
    )
    def _asset(postgis_resource: PostGISResource) -> gpd.GeoDataFrame:
        conn = postgis_resource.get_connection()
        return gpd.read_postgis(f"census_{year}_mun", con=conn, geom_col="geometry")

    return _asset


assets = [muns_factory(year) for year in [2020]]
