import shapely

import geopandas as gpd
import networkx as nx
import pandas as pd

from ageb_alignment.partitions import zone_partitions
from dagster import graph_asset, op, AssetsDefinition, AssetIn, Out


@op
def get_vertices(gdf: gpd.GeoDataFrame) -> set:
    points = gdf.sjoin(gdf, how="inner", predicate="touches")

    g = nx.Graph()
    for row in points.itertuples():
        g.add_edge(row.CVEGEO_left, row.CVEGEO_right)

    vertices = []
    for clique in nx.enumerate_all_cliques(g):
        if len(clique) > 2:
            vertices.append(frozenset(clique))
    return set(vertices)


@op
def get_clique_geometries(gdf: gpd.GeoDataFrame, points: set) -> dict:
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
        if not isinstance(intersection, shapely.geometry.Point):
            continue
        clique_geometries[clique] = intersection
    return clique_geometries


@op(out=Out(io_manager_key="points_manager"))
def merge_columns(source: dict, target: dict) -> pd.DataFrame:
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


def initial_gcp_factory(year: int) -> AssetsDefinition:
    @graph_asset(
        name=str(year),
        key_prefix=["gcp"],
        ins={
            "df_source": AssetIn(key=["zones_extended", str(year)]),
            "df_target": AssetIn(key=["zones_extended", str(year + 10)]),
        },
        partitions_def=zone_partitions,
    )
    def _asset(
        df_source: gpd.GeoDataFrame,
        df_target: gpd.GeoDataFrame,
    ) -> pd.DataFrame:
        sources = get_vertices(df_source)
        targets = get_vertices(df_target)

        source_geoms = get_clique_geometries(df_source, sources)
        target_geoms = get_clique_geometries(df_target, targets)

        merged = merge_columns(source_geoms, target_geoms)
        return merged

    return _asset


initial_gcp_assets = [
    initial_gcp_factory(year)
    for year in (
        1990,
        2000,
    )
]
