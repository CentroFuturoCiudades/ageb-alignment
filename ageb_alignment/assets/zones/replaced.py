import geopandas as gpd

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource, AgebNestedDictResource
from dagster import asset, AssetDep, AssetExecutionContext
from pathlib import Path


def zones_replaced_factory(year: int) -> asset:
    @asset(
        name=str(year),
        key_prefix=["zone_agebs", "replaced"],
        deps=[AssetDep(["zone_agebs", "shaped", str(year)])],
        partitions_def=zone_partitions,
    )
    def _asset(
        context: AssetExecutionContext,
        path_resource: PathResource,
        replacement_resource: AgebNestedDictResource,
    ) -> None:
        zone = context.partition_key
        root_out_path = Path(path_resource.out_path)

        replaced_path = root_out_path / f"zone_agebs_replaced/{year}"
        replaced_path.mkdir(exist_ok=True, parents=True)

        df = gpd.read_file(root_out_path / f"zone_agebs_shaped/{year}/{zone}.gpkg")

        if zone in replacement_resource:
            ageb_map = replacement_resource[zone]
            for old_ageb, new_agebs in ageb_map.items():
                new_geometry = df.loc[new_agebs, "geometry"].union_all()
                if new_geometry.geom_type != "Polygon":
                    raise Exception("Non-Polygon created.")

                df.loc[old_ageb, "geometry"] = new_geometry

        df.to_file(replaced_path / f"{zone}.gpkg")

    return _asset


zones_replaced_assets = [
    zones_replaced_factory(year) for year in (1990, 2000, 2010, 2020)
]
