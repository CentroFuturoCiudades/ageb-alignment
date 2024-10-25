import geopandas as gpd
import pandas as pd

from ageb_alignment.resources import PathResource, PreferenceResource
from dagster import graph_asset, op, AssetIn
from pathlib import Path


@op
def load_mesh(
    path_resource: PathResource, preference_resource: PreferenceResource
) -> gpd.GeoDataFrame:
    mesh_root_path = Path(path_resource.raw_path) / "mesh"
    mesh = []
    for path in mesh_root_path.glob(f"nivel{preference_resource.mesh_level}*.shp"):
        mesh.append(gpd.read_file(path, engine="pyogrio"))
    mesh = pd.concat(mesh)
    return mesh


# pylint: disable=no-value-for-parameter
@graph_asset(ins={})
def reprojected():
    mesh = load_mesh()
