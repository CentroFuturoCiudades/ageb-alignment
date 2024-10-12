import geopandas as gpd
import numpy as np

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import AgebDictResource, PathResource
from dagster import asset, AssetExecutionContext, BackfillPolicy, ExperimentalWarning
from pathlib import Path


# Suppress experimental warnings
import warnings

warnings.filterwarnings("ignore", category=ExperimentalWarning)


def _load_agebs_and_prep_paths(root_out_path: Path, year: int) -> gpd.GeoDataFrame:
    out_path = root_out_path / f"zone_agebs/{year}"
    out_path.mkdir(exist_ok=True, parents=True)

    ageb_path = root_out_path / f"framework/agebs/{year}.gpkg"
    df = (
        gpd.read_file(ageb_path)
        .assign(CVEGEO_MUN=lambda df: df.CVEGEO.str[:5])
        .set_index("CVEGEO")
    )
    return df, out_path


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
    deps=["agebs_2020"],
    partitions_def=zone_partitions,
    backfill_policy=BackfillPolicy.single_run(),
)
def zone_agebs_2020(
    context: AssetExecutionContext, path_resource: PathResource, metropoli_list: dict
) -> None:
    root_out_path = Path(path_resource.out_path)
    df, out_path = _load_agebs_and_prep_paths(root_out_path, 2020)

    for zone in context.partition_keys:
        mun_list = metropoli_list[zone]
        zone_agebs = _extract_and_fix_geometry(df, mun_list)
        zone_agebs = zone_agebs.to_crs("EPSG:4326")
        zone_agebs.to_file(out_path / f"{zone}.geojson")


def zone_agebs_factory(year: int) -> asset:
    @asset(
        deps=[f"agebs_{year}", "municipalities_2020"],
        name=f"zone_agebs_{year}",
        partitions_def=zone_partitions,
        backfill_policy=BackfillPolicy.single_run(),
    )
    def _asset(
        context: AssetExecutionContext,
        path_resource: PathResource,
        remove_from_mun_resource: AgebDictResource,
        metropoli_list: dict,
    ) -> None:
        root_out_path = Path(path_resource.out_path)
        df, out_path = _load_agebs_and_prep_paths(root_out_path, year)
        municipalities_2020 = gpd.read_file(
            root_out_path / "framework/municipalities/2020.gpkg"
        )

        remove_dict = getattr(remove_from_mun_resource, f"ageb_{year}")

        for zone in context.partition_keys:
            mun_list = metropoli_list[zone]
            zone_agebs = _extract_and_fix_geometry(df, mun_list)

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
            zone_agebs.to_file(out_path / f"{zone}.geojson")

    return _asset


zone_agebs_assets = [zone_agebs_factory(year) for year in (1990, 2000, 2010)]
