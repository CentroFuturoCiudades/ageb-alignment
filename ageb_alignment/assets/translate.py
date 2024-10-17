import os

import numpy as np

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource
from dagster import asset, AssetExecutionContext, AssetIn
from osgeo import gdal
from pathlib import Path


gdal.UseExceptions()


def generate_options_str(gcp: np.ndarray) -> str:
    options_str = "-tps -t_srs EPSG:6372 "
    for row in gcp:
        options_str += "-gcp " + " ".join(row.astype(str)) + " "
    return options_str


def prep_dir(path_resource: PathResource, context: AssetExecutionContext):
    out_dir = Path(path_resource.out_path) / "/".join(context.asset_key.path)
    out_dir.mkdir(exist_ok=True, parents=True)
    return out_dir


@asset(
    name="2000",
    key_prefix=["translated"],
    ins={
        "gcp_2000": AssetIn(key=["gcp", "final", "2000"]),
        "ageb_path": AssetIn(
            key=["zone_agebs", "shaped", "2000"], input_manager_key="path_gpkg_manager"
        ),
    },
    partitions_def=zone_partitions,
)
def translated_2000(
    context: AssetExecutionContext,
    path_resource: PathResource,
    gcp_2000: np.ndarray,
    ageb_path: Path,
) -> None:
    options_str = generate_options_str(gcp_2000)
    out_dir = prep_dir(path_resource, context)
    out_path = out_dir / f"{context.partition_key}.gpkg"
    gdal.VectorTranslate(str(out_path), str(ageb_path), options=options_str)


@asset(
    name="1990",
    key_prefix=["translated"],
    ins={
        "gcp_1990": AssetIn(key=["gcp", "final", "1990"]),
        "gcp_2000": AssetIn(key=["gcp", "final", "2000"]),
        "ageb_path": AssetIn(
            key=["zone_agebs", "shaped", "1990"], input_manager_key="path_gpkg_manager"
        ),
    },
    partitions_def=zone_partitions,
)
def translated_1990(
    context: AssetExecutionContext,
    path_resource: PathResource,
    gcp_1990: np.ndarray,
    gcp_2000: np.ndarray,
    ageb_path: Path,
) -> None:
    options_str_1990 = generate_options_str(gcp_1990)
    options_str_2000 = generate_options_str(gcp_2000)

    out_dir = prep_dir(path_resource, context)
    temp_path = out_dir / f"{context.partition_key}_temp.gpkg"
    out_path = out_dir / f"{context.partition_key}.gpkg"

    gdal.VectorTranslate(str(temp_path), str(ageb_path), options=options_str_1990)
    gdal.VectorTranslate(str(out_path), str(temp_path), options=options_str_2000)

    os.remove(temp_path)
