import shapely

import geopandas as gpd
import networkx as nx
import pandas as pd

from ageb_alignment.partitions import zone_partitions
from dagster import asset, AssetsDefinition, AssetIn


def get_vertices(gdf):
    points = gdf.sjoin(gdf, how="inner", predicate="touches")

    g = nx.Graph()
    for row in points.itertuples():
        g.add_edge(row.CVEGEO_left, row.CVEGEO_right)

    vertices = []
    for clique in nx.enumerate_all_cliques(g):
        if len(clique) > 2:
            vertices.append(frozenset(clique))
    return set(vertices)


def get_clique_geometries(gdf: gpd.GeoDataFrame, points, *, only_points=False):
    series = gdf.set_index("CVEGEO")
    series = series["geometry"]

    clique_geometries = {}
    for clique in points:
        geometries = []
        for elem in clique:
            geometry = series.loc[elem]
            geometries.append(geometry)

        intersection = shapely.intersection_all(geometries)
        if intersection.is_empty:
            continue
        if only_points and not isinstance(intersection, shapely.geometry.Point):
            continue
        clique_geometries[clique] = intersection
    return clique_geometries


def merge_columns(source, target):
    temp_source = pd.Series(source, name="source")
    temp_target = pd.Series(target, name="target")

    merged = pd.concat([temp_source, temp_target], axis=1)
    merged = merged.dropna()
    merged["sourceX"] = merged["source"].apply(lambda x: x.coords.xy[0][0])
    merged["sourceY"] = merged["source"].apply(lambda x: x.coords.xy[1][0])
    merged["mapX"] = merged["target"].apply(lambda x: x.coords.xy[0][0])
    merged["mapY"] = merged["target"].apply(lambda x: x.coords.xy[1][0])
    merged = merged[["mapX", "mapY", "sourceX", "sourceY"]]
    merged["enable"] = 1
    merged["dX"] = 0
    merged["dY"] = 0
    merged["residual"] = 0
    merged = merged.drop_duplicates(subset=["mapX", "mapY", "sourceX", "sourceY"])
    merged = merged.drop_duplicates(subset=["sourceX", "sourceY"])
    merged = merged.drop_duplicates(subset=["mapX", "mapY"])

    return merged


def process_df_pair(
    df_source: gpd.GeoDataFrame, df_target: gpd.GeoDataFrame
) -> pd.DataFrame:
    sources = get_vertices(df_source)
    targets = get_vertices(df_target)

    source_geoms = get_clique_geometries(df_source, sources, only_points=True)
    target_geoms = get_clique_geometries(df_target, targets, only_points=True)

    merged = merge_columns(source_geoms, target_geoms)
    return merged


def initial_gcp_factory(year: int) -> AssetsDefinition:
    @asset(
        name=str(year),
        key_prefix=["gcp", "initial"],
        ins={"census_extended": AssetIn(key=["zones_extended", str(year)])},
        partitions_def=zone_partitions,
        io_manager_key="points_manager",
    )
    def _asset(
        census_extended: tuple,
    ) -> pd.DataFrame:
        df_source = census_extended[0]
        df_target = census_extended[1]
        merged = process_df_pair(df_source, df_target)
        return merged

    return _asset


initial_gcp_assets = [initial_gcp_factory(year) for year in (2000,)]
