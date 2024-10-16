import os
import subprocess

import geopandas as gpd

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource, PreferenceResource
from dagster import asset, AssetDep, AssetExecutionContext
from pathlib import Path


def zone_agebs_shaped_factory(year: int) -> asset:
    @asset(
        name=str(year),
        key_prefix=["zone_agebs", "shaped"],
        deps=[AssetDep(["zone_agebs", "replaced", str(year)])],
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
    )
    def _asset(
        context: AssetExecutionContext,
        path_resource: PathResource,
        preference_resource: PreferenceResource,
    ) -> gpd.GeoDataFrame:
        zone = context.partition_key
        (dep_key,) = context.assets_def.asset_deps[context.asset_key]

        out_root_path = Path(path_resource.out_path)

        in_path = out_root_path / "/".join(dep_key.path) / f"{zone}.geojson"

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
                f"{quote}{in_path}{quote}",
                "-clean",
                "-o",
                f"{quote}{out_path_json}{quote}",
            ],
            shell=shell,
        )

        df = gpd.read_file(out_path_json)
        df_orig = gpd.read_file(in_path)

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
