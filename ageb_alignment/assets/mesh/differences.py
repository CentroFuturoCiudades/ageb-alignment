import geopandas as gpd

from ageb_alignment.partitions import zone_partitions
from dagster import asset
from itertools import combinations


# pylint: disable=deprecated-method
@asset(partitions_def=zone_partitions)
def differences(reprojected: gpd.GeoDataFrame):
    years = (1990, 2000, 2010, 2020)
    for start_year, end_year in combinations(years, 2):
        if start_year > end_year:
            start_year, end_year = end_year, start_year

        start_year = str(start_year)
        end_year = str(end_year)

        reprojected = reprojected.dropna(subset=[start_year, end_year], how="all")
        reprojected = reprojected[
            ~((reprojected[start_year].notna()) & (reprojected[end_year].isna()))
        ]
        reprojected = reprojected.fillna(0)
        reprojected = reprojected[
            ~(reprojected[[start_year, end_year]] == 0).all(axis=1)
        ]
        reprojected["difference"] = reprojected[end_year] - reprojected[start_year]
        reprojected = reprojected[["difference", "geometry"]]
        return reprojected
