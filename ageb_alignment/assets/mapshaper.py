import os
import subprocess

import geopandas as gpd

from ageb_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


def zone_agebs_clean_factory(year: int) -> asset:
    @asset(name=f"zone_agebs_fixed_{year}", deps=[f"zone_agebs_{year}"])
    def _asset(path_resource: PathResource) -> None:
        out_root_path = Path(path_resource.out_path)

        zone_agebs_path = out_root_path / f"zone_agebs/{year}"

        out_dir = out_root_path / f"zone_agebs_fixed/{year}"
        out_dir.mkdir(exist_ok=True, parents=True)

        for path in zone_agebs_path.glob("*.geojson"):
            zone = path.stem

            out_path_json = out_dir / f"{zone}.geojson"
            out_path_gpkg = out_dir / f"{zone}.gpkg"

            subprocess.check_call(
                [
                    "npx",
                    "mapshaper",
                    "-i",
                    f'"{path}"',
                    "-clean",
                    "-o",
                    f'"{out_path_json}"',
                ],
                shell=True,
            )

            df = gpd.read_file(out_path_json)
            df = df.to_crs("EPSG:6372")
            df.to_file(out_path_gpkg)

            os.remove(out_path_json)

    return _asset


zone_agebs_clean_assets = [
    zone_agebs_clean_factory(year) for year in (1990, 2000, 2010, 2020)
]
