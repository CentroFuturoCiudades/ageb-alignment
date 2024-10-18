import os
import subprocess

import geopandas as gpd

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource, PreferenceResource
from dagster import asset, AssetIn, AssetExecutionContext
from pathlib import Path


def zone_agebs_shaped_factory(year: int) -> asset:
    key_infix = None
    if year in (1990, 2000):
        key_infix = "replaced"
    elif year in (2010, 2020):
        key_infix = "initial"

    @asset(
        name=str(year),
        key_prefix=["zone_agebs", "shaped"],
        ins={
            "ageb_path": AssetIn(
                key=["zone_agebs", key_infix, str(year)],
                input_manager_key="path_geojson_manager",
            )
        },
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
    )
    def _asset(
        context: AssetExecutionContext,
        path_resource: PathResource,
        preference_resource: PreferenceResource,
        ageb_path: Path,
    ) -> gpd.GeoDataFrame:
        zone = context.partition_key
        out_root_path = Path(path_resource.out_path)

        out_dir = out_root_path / "/".join(context.asset_key.path)
        out_dir.mkdir(exist_ok=True, parents=True)

        out_path_json = out_dir / f"{zone}.geojson"

        if os.name == "nt":
            shell = True
            quote = '"'
        else:
            shell = False
            quote = ""

        subprocess.check_call(
            [
                "npx",
                "mapshaper",
                "-i",
                f"{quote}{ageb_path}{quote}",
                "-clean",
                "-o",
                f"{quote}{out_path_json}{quote}",
            ],
            shell=shell,
        )

        df = gpd.read_file(out_path_json)
        df_orig = gpd.read_file(ageb_path)

        os.remove(out_path_json)

        if len(df) != len(df_orig):
            if preference_resource.raise_on_deleted_geometries:
                raise Exception("Geometries were deleted.")
            else:
                context.warning(f"Geometries for zone {zone} were deleted.")

        df = df.to_crs("EPSG:6372")
        return df

    return _asset


zone_agebs_clean = [
    zone_agebs_shaped_factory(year) for year in (1990, 2000, 2010, 2020)
]
