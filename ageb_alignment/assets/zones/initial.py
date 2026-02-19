import geopandas as gpd
import pandas as pd

import dagster as dg
from ageb_alignment.partitions import zone_partitions

# def _remove_not_in_mun(gdf: gpd.GeoDataFrame, mun_gdf: gpd.GeoDataFrame):
#     """Removes geometries in gdf not intersecting municipalities in mun_gdf."""
#     gdf = gdf.copy()
#     keep = np.zeros(len(gdf), dtype=bool)
#     for mun in mun_gdf.geometry:
#         keep = np.logical_or(keep, gdf.intersects(mun))
#     return gdf[keep]
#
#
# def process_agebs(agebs: gpd.GeoDataFrame, mun_list: list) -> gpd.GeoDataFrame:
#     return (
#         agebs
#         .assign(CVEGEO_MUN=lambda df: df.CVEGEO.str[:5])
#         .set_index("CVEGEO")
#         .query("CVEGEO_MUN in @mun_list")
#         [["POBTOT", "geometry"]]
#         .assign(geometry=lambda df: df["geometry"].make_valid())
#     )


# def zone_agebs_factory(year: int) -> dg.AssetsDefinition:
#     @dg.asset(
#         ins={
#             "agebs": dg.AssetIn(key=["framework", "agebs", str(year)]),
#             "municipalities_2020": dg.AssetIn(key=["framework", "mun", "2020"]),
#             "metropoli_list": dg.AssetIn(key=["metropoli", "list"]),
#         },
#         name=str(year),
#         key_prefix=["zone_agebs", "initial"],
#         partitions_def=zone_partitions,
#         io_manager_key="geojson_manager",
#     )
#     def _asset(
#         context: dg.AssetExecutionContext,
#         remove_from_mun_resource: AgebDictResource,
#         metropoli_list: dict[str, list],
#         agebs: gpd.GeoDataFrame,
#         municipalities_2020: gpd.GeoDataFrame,
#     ) -> gpd.GeoDataFrame:
#         zone = context.partition_key
#         mun_list = metropoli_list[zone]
#
#         zone_agebs = process_agebs(agebs, mun_list)
#
#         mun_list_trimmed = [int(m) for m in mun_list]
#         mun_gdf = municipalities_2020[
#             municipalities_2020["CVEGEO"].isin(mun_list_trimmed)
#         ]
#
#         remove_dict = getattr(remove_from_mun_resource, f"ageb_{year}")
#         if zone in remove_dict:
#             for ageb in remove_dict[zone]:
#                 if ageb == "intersect":
#                     zone_agebs = _remove_not_in_mun(zone_agebs, mun_gdf)
#                 else:
#                     zone_agebs = zone_agebs.drop(ageb)
#
#         zone_agebs = zone_agebs.to_crs("EPSG:4326")
#         return zone_agebs
#
#     return _asset
#
#
# @dg.asset(
#     name="2020",
#     key_prefix=["zone_agebs", "initial"],
#     ins={"agebs": dg.AssetIn(key=["framework", "agebs", "2020"]), "metropoli_list": dg.AssetIn(key=["metropoli", "list"])},
#     partitions_def=zone_partitions,
#     io_manager_key="geojson_manager",
# )
# def zone_agebs_2020(
#     context: dg.AssetExecutionContext,
#     metropoli_list: dict,
#     agebs: gpd.GeoDataFrame,
# ) -> gpd.GeoDataFrame:
#     zone = context.partition_key
#     mun_list = metropoli_list[zone]
#     return process_agebs(agebs, mun_list).to_crs("EPSG:4326")


def remove_multipoly(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    counts = df.explode()["CVEGEO"].value_counts()
    idx = counts[counts > 1].to_dict()

    nonrepeated = df[~df["CVEGEO"].isin(idx.keys())].explode()

    repeated = []
    for cvegeo in idx.keys():
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
        name=str(year),
        key_prefix=["zone_agebs", "initial"],
        ins={
            "agebs": dg.AssetIn(key=["framework", "agebs", str(year)]),
            "municipalities_2020": dg.AssetIn(key=["framework", "mun", "2020"]),
            "metropoli_list": dg.AssetIn(key=["metropoli", "list"]),
        },
        partitions_def=zone_partitions,
        io_manager_key="geojson_manager",
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
