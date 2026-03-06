import geopandas as gpd
import pandas as pd

import dagster as dg
from ageb_alignment.defs.partitions import zone_partitions


def remove_multipoly(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    counts = df.explode()["CVEGEO"].value_counts()
    idx = counts[counts > 1].to_dict()

    nonrepeated = df[~df["CVEGEO"].isin(idx.keys())].explode()

    repeated = []
    for cvegeo in idx:
        temp = (
            df[df["CVEGEO"] == cvegeo]
            .explode()
            .assign(
                POBTOT=lambda df: df["POBTOT"] / counts[cvegeo],
                suffix=[chr(x + 65) for x in range(counts[cvegeo])],
                CVEGEO=lambda df: df["CVEGEO"] + "_" + df["suffix"],
            )
            .drop(columns=["suffix"])
        )
        repeated.append(temp)

    return gpd.GeoDataFrame(pd.concat([nonrepeated] + repeated, ignore_index=True))


def zone_agebs_factory(year: int) -> dg.AssetsDefinition:
    @dg.asset(
        key=["zone_agebs", "initial", str(year)],
        ins={
            "agebs": dg.AssetIn(key=["framework", "agebs", str(year)]),
            "municipalities_2020": dg.AssetIn(key=["framework", "mun", "2020"]),
            "metropoli_list": dg.AssetIn(key=["metropoli", "list"]),
        },
        partitions_def=zone_partitions,
        io_manager_key="geojson_manager",
        group_name="initial",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        metropoli_list: dict[str, list],
        agebs: gpd.GeoDataFrame,
        municipalities_2020: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        zone = context.partition_key
        mun_list = metropoli_list[zone]

        muns_in_zone = municipalities_2020[municipalities_2020["CVEGEO"].isin(mun_list)]

        wanted_idx = agebs.sjoin(
            muns_in_zone[["geometry"]],
            how="inner",
            predicate="intersects",
        ).index.unique()

        out = agebs.loc[wanted_idx]
        return remove_multipoly(out).to_crs("EPSG:4326")

    return _asset


zone_agebs_assets = [zone_agebs_factory(year) for year in (1990, 2000, 2010, 2020)]
