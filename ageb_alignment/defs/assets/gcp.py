import geopandas as gpd
import networkx as nx
import pandas as pd
import shapely

import dagster as dg
from ageb_alignment.defs.partitions import zone_partitions


@dg.op
def get_vertices(gdf: gpd.GeoDataFrame) -> set:
    points = gdf.sjoin(gdf, how="inner", predicate="touches")

    g = nx.Graph()
    for row in points.itertuples():
        g.add_edge(row.CVEGEO_left, row.CVEGEO_right)

    vertices = [
        frozenset(clique) for clique in nx.enumerate_all_cliques(g) if len(clique) > 2
    ]
    return set(vertices)


@dg.op
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


@dg.op(out=dg.Out(io_manager_key="points_manager"))
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
    return merged.drop_duplicates(subset=["mapX", "mapY"])


def initial_gcp_factory(source_year: int, target_year: int) -> dg.AssetsDefinition:
    @dg.graph_asset(
        key=["gcp", str(source_year)],
        ins={
            "df_source": dg.AssetIn(key=["zone_agebs", "extended", str(source_year)]),
            "df_target": dg.AssetIn(key=["zone_agebs", "extended", str(target_year)]),
        },
        partitions_def=zone_partitions,
        group_name="gcp",
    )
    def _asset(
        df_source: gpd.GeoDataFrame,
        df_target: gpd.GeoDataFrame,
    ) -> pd.DataFrame:
        sources = get_vertices(df_source)
        targets = get_vertices(df_target)

        source_geoms = get_clique_geometries(df_source, sources)
        target_geoms = get_clique_geometries(df_target, targets)

        return merge_columns(source_geoms, target_geoms)

    return _asset


initial_gcp_assets = [
    initial_gcp_factory(1990, 2010),
    initial_gcp_factory(2000, 2010),
]
