import geopandas as gpd

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import AgebNestedDictResource
from dagster import asset, AssetExecutionContext, AssetIn


def zones_replaced_factory(year: int) -> asset:
    @asset(
        name=str(year),
        key_prefix=["zone_agebs", "replaced"],
        ins={"agebs_initial": AssetIn(["zone_agebs", "initial", str(year)])},
        partitions_def=zone_partitions,
        io_manager_key="geojson_manager",
    )
    def _asset(
        context: AssetExecutionContext,
        replacement_resource: AgebNestedDictResource,
        agebs_initial: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        zone = context.partition_key

        if zone in replacement_resource:
            ageb_map = replacement_resource[zone]
            for old_ageb, new_agebs in ageb_map.items():
                new_geometry = agebs_initial.loc[new_agebs, "geometry"].union_all()
                if new_geometry.geom_type != "Polygon":
                    raise Exception("Non-Polygon created.")

                agebs_initial.loc[old_ageb, "geometry"] = new_geometry

        return agebs_initial

    return _asset


zones_replaced_assets = [
    zones_replaced_factory(year) for year in (1990, 2000, 2010, 2020)
]
