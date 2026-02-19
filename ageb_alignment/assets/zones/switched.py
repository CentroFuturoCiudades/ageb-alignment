import geopandas as gpd
import pandas as pd

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import AgebDictResource
from dagster import (
    AllPartitionMapping,
    AssetExecutionContext,
    AssetIn,
    AssetsDefinition,
    asset,
)


def zones_switched_factory(year: int) -> AssetsDefinition:
    @asset(
        name=str(year),
        key_prefix=["zone_agebs", "switched"],
        ins={
            "all_agebs": AssetIn(
                key=["zone_agebs", "initial", str(year)],
                partition_mapping=AllPartitionMapping(),
            ),
        },
        io_manager_key="geojson_manager",
        partitions_def=zone_partitions,
    )
    def _asset(
        context: AssetExecutionContext,
        switch_resource: AgebDictResource,
        all_agebs: dict[str, gpd.GeoDataFrame],
    ):
        zone = context.partition_key
        agebs = all_agebs[zone]
        switches: dict = getattr(switch_resource, f"ageb_{year}")

        # Zone is giving AGEBs
        if zone in switches:
            dropped_agebs = []
            for ageb_list in switches[zone].values():
                dropped_agebs.extend(ageb_list)
            agebs = agebs[~agebs["CVEGEO"].isin(dropped_agebs)]
            context.log.info(f"Dropped AGEBs {dropped_agebs} for zone {zone}.")

        # Zone is receiving AGEBs
        for giver, receiver_map in switches.items():
            for receiver, wanted_agebs in receiver_map.items():
                if zone == receiver:
                    giver_df = all_agebs[giver]
                    giver_agebs = giver_df[giver_df["CVEGEO"].isin(wanted_agebs)]
                    agebs = pd.concat([agebs, giver_agebs])
                    context.log.info(
                        f"Received AGEBs {wanted_agebs} from zone {giver}.",
                    )

        return agebs

    return _asset


zones_switched_assets = [zones_switched_factory(year) for year in (1990,)]
