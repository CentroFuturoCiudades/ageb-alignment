import geopandas as gpd

from ageb_alignment.partitions import zone_partitions
from dagster import asset, AssetIn, AssetsDefinition
from itertools import combinations


# pylint: disable=deprecated-method
def differences_factory(start_year: int, end_year: int) -> AssetsDefinition:
    start_year, end_year = str(start_year), str(end_year)

    @asset(
        name=f"{start_year}_{end_year}",
        key_prefix="differences",
        ins={"merged": AssetIn(key=["reprojected", "merged"])},
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
    )
    def _asset(merged: gpd.GeoDataFrame):
        merged = merged[["codigo", start_year, end_year, "geometry"]].copy()
        merged = merged.dropna(subset=[start_year, end_year], how="all")
        merged = merged[~((merged[start_year].notna()) & (merged[end_year].isna()))]

        merged[start_year] = merged[start_year].fillna(0)
        merged[end_year] = merged[end_year].fillna(0)

        merged = merged[~(merged[[start_year, end_year]] == 0).all(axis=1)]
        merged["difference"] = merged[end_year] - merged[start_year]
        merged = merged.drop(columns=[start_year, end_year])
        return merged

    return _asset


differences_assets = []
for start_year, end_year in combinations((1990, 2000, 2010, 2020), 2):
    if start_year > end_year:
        start_year, end_year = end_year, start_year
    differences_assets.append(differences_factory(start_year, end_year))
