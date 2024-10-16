import geopandas as gpd
import numpy as np

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import AgebDictResource
from dagster import (
    asset,
    AssetExecutionContext,
    AssetIn,
    ExperimentalWarning,
)
from pathlib import Path


# Suppress experimental warnings
import warnings

warnings.filterwarnings("ignore", category=ExperimentalWarning)


def _process_agebs(agebs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return agebs.assign(CVEGEO_MUN=lambda df: df.CVEGEO.str[:5]).set_index("CVEGEO")


def _prep_paths(root_out_path: Path, year: int) -> gpd.GeoDataFrame:
    out_path = root_out_path / f"zone_agebs/initial/{year}"
    out_path.mkdir(exist_ok=True, parents=True)
    return out_path


def _extract_and_fix_geometry(df: gpd.GeoDataFrame, mun_list: list) -> gpd.GeoDataFrame:
    zone_agebs = df[df["CVEGEO_MUN"].isin(mun_list)]
    zone_agebs = zone_agebs[["POBTOT", "geometry"]]
    zone_agebs["geometry"] = zone_agebs["geometry"].make_valid()
    return zone_agebs


def _remove_not_in_mun(gdf: gpd.GeoDataFrame, mun_gdf: gpd.GeoDataFrame):
    """Removes geoemtries in gdf not intersecting municipalities in mun_gdf."""
    gdf = gdf.copy()
    keep = np.zeros(len(gdf), dtype=bool)
    for mun in mun_gdf.geometry:
        keep = np.logical_or(keep, gdf.intersects(mun))
    return gdf[keep]


@asset(
    name="2020",
    key_prefix=["zone_agebs", "initial"],
    ins={"agebs": AssetIn(key=["framework", "agebs", "2020"])},
    partitions_def=zone_partitions,
    io_manager_key="geojson_manager",
)
def zone_agebs_2020(
    context: AssetExecutionContext,
    metropoli_list: dict,
    agebs: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    zone = context.partition_key

    agebs = _process_agebs(agebs)
    mun_list = metropoli_list[zone]
    zone_agebs = _extract_and_fix_geometry(agebs, mun_list)
    zone_agebs = zone_agebs.to_crs("EPSG:4326")
    return zone_agebs


def zone_agebs_factory(year: int) -> asset:
    @asset(
        ins={
            "agebs": AssetIn(key=["framework", "agebs", str(year)]),
            "municipalities_2020": AssetIn(key=["framework", "municipalities", "2020"]),
        },
        name=str(year),
        key_prefix=["zone_agebs", "initial"],
        partitions_def=zone_partitions,
        io_manager_key="geojson_manager",
    )
    def _asset(
        context: AssetExecutionContext,
        remove_from_mun_resource: AgebDictResource,
        metropoli_list: dict,
        agebs: gpd.GeoDataFrame,
        municipalities_2020: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        zone = context.partition_key

        agebs = _process_agebs(agebs)

        remove_dict = getattr(remove_from_mun_resource, f"ageb_{year}")

        mun_list = metropoli_list[zone]
        zone_agebs = _extract_and_fix_geometry(agebs, mun_list)

        mun_list_trimmed = [int(m) for m in mun_list]
        mun_gdf = municipalities_2020[
            municipalities_2020["CVEGEO"].isin(mun_list_trimmed)
        ]
        if zone in remove_dict:
            for ageb in remove_dict[zone]:
                if ageb == "intersect":
                    zone_agebs = _remove_not_in_mun(zone_agebs, mun_gdf)
                else:
                    zone_agebs = zone_agebs.drop(ageb)

        zone_agebs = zone_agebs.to_crs("EPSG:4326")
        return zone_agebs

    return _asset


zone_agebs_assets = [zone_agebs_factory(year) for year in (1990, 2000, 2010)]
