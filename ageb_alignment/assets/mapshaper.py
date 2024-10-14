import os
import subprocess

import geopandas as gpd

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource, PreferenceResource
from dagster import asset, AssetExecutionContext
from pathlib import Path


def zone_agebs_clean_factory(year: int) -> asset:
    name = f"zone_agebs_fixed_{year}"

    @asset(name=name, deps=[f"zone_agebs_{year}"], partitions_def=zone_partitions)
    def _asset(
        context: AssetExecutionContext,
        path_resource: PathResource,
        preference_resource: PreferenceResource,
    ) -> None:
        zone = context.partition_key

        out_root_path = Path(path_resource.out_path)

        in_path = out_root_path / f"zone_agebs/{year}/{zone}.geojson"

        out_dir = out_root_path / f"zone_agebs_fixed/{year}"
        out_dir.mkdir(exist_ok=True, parents=True)

        out_path_json = out_dir / f"{zone}.geojson"
        out_path_gpkg = out_dir / f"{zone}.gpkg"

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
        df.to_file(out_path_gpkg)

    return _asset


zone_agebs_clean = [zone_agebs_clean_factory(year) for year in (1990, 2000, 2010, 2020)]
