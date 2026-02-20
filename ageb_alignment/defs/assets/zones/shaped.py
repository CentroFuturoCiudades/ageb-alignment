import os
import subprocess
from pathlib import Path

import geopandas as gpd

import dagster as dg
from ageb_alignment.defs.partitions import zone_partitions
from ageb_alignment.defs.resources import PathResource, PreferenceResource


def zone_agebs_shaped_factory(year: int) -> dg.AssetsDefinition:
    @dg.asset(
        key=["zone_agebs", "shaped", str(year)],
        ins={
            "ageb_path": dg.AssetIn(
                key=["zone_agebs", "initial", str(year)],
                input_manager_key="path_geojson_manager",
            ),
        },
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
        group_name="shaped",
    )
    def _asset(
        context: dg.AssetExecutionContext,
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
            quote = ""
        else:
            shell = False
            quote = ""

        try:
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
        except subprocess.CalledProcessError as e:
            print(e.output)
            raise

        df = gpd.read_file(out_path_json)
        df_orig = gpd.read_file(ageb_path)

        os.remove(out_path_json)

        if len(df) != len(df_orig):
            if preference_resource.raise_on_deleted_geometries:
                raise Exception("Geometries were deleted.")
            context.log.warning(f"Geometries for zone {zone} were deleted.")

        return df.to_crs("EPSG:6372")

    return _asset


zone_agebs_clean = [
    zone_agebs_shaped_factory(year) for year in (1990, 2000, 2010, 2020)
]
