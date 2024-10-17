import numpy as np

from ageb_alignment.partitions import zone_partitions
from ageb_alignment.resources import PathResource
from dagster import asset, AssetExecutionContext, AssetIn
from osgeo import gdal
from pathlib import Path


gdal.UseExceptions()


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
    options_str = "-tps -t_srs EPSG:6372 "
    for row in gcp_2000:
        options_str += "-gcp " + " ".join(row.astype(str)) + " "

    out_dir = Path(path_resource.out_path) / "translated/2000"
    out_dir.mkdir(exist_ok=True, parents=True)

    out_path = out_dir / f"{context.partition_key}.gpkg"
    gdal.VectorTranslate(str(out_path), str(ageb_path), options=options_str)
