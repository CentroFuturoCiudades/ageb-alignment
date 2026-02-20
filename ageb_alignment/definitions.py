from pathlib import Path

import toml

import dagster as dg
from ageb_alignment.defs.jobs import (
    generate_framework_job,
    generate_initial_gcp_job,
    pipeline_1_job,
    pipeline_2_job,
)
from ageb_alignment.defs.managers import (
    DataFrameIOManager,
    JSONIOManager,
    PathIOManager,
)
from ageb_alignment.defs.resources import (
    AgebDictResource,
    AgebListResource,
    AgebNestedDictResource,
    PathResource,
    PreferenceResource,
)

# Resources
path_resource = PathResource(
    data_path=dg.EnvVar("DATA_PATH"),
    ghsl_path=dg.EnvVar("GHSL_GLOBAL_PATH"),
)

with Path("./configs/overlaps.toml").open(encoding="utf8") as f:
    overlap_list = toml.load(f)
overlap_list = {f"ageb_{key}": value for key, value in overlap_list.items()}
overlap_resource = AgebListResource(**overlap_list)

with Path("./configs/remove_from_mun.toml").open(encoding="utf8") as f:
    remove_from_mun_list = toml.load(f)
remove_from_mun_list = {
    f"ageb_{key}": value for key, value in remove_from_mun_list.items()
}
remove_from_mun_resource = AgebDictResource(**remove_from_mun_list)

with Path("./configs/preferences.toml").open(encoding="utf8") as f:
    preferences = toml.load(f)
preference_resource = PreferenceResource(
    raise_on_deleted_geometries=preferences["raise_on_deleted_geometries"],
    mesh_level=preferences["mesh_level"],
)

with Path("./configs/switches.toml").open(encoding="utf8") as f:
    switch_list = toml.load(f)
switch_list = {f"ageb_{key}": value for key, value in switch_list.items()}
switch_resource = AgebNestedDictResource(**switch_list)


with Path("./configs/affine.toml").open(encoding="utf8") as f:
    rigid_list = toml.load(f)
rigid_list = {f"ageb_{key}": value for key, value in rigid_list.items()}
affine_resource = AgebDictResource(**rigid_list)


# Managers
gpkg_manager = DataFrameIOManager(path_resource=path_resource, extension=".gpkg")
geojson_manager = DataFrameIOManager(path_resource=path_resource, extension=".geojson")
points_manager = DataFrameIOManager(
    path_resource=path_resource,
    extension=".points",
    with_index=False,
)
csv_manager = DataFrameIOManager(path_resource=path_resource, extension=".csv")
json_manager = JSONIOManager(path_resource=path_resource, extension=".json")

path_geojson_manager = PathIOManager(path_resource=path_resource, extension=".geojson")
path_gpkg_manager = PathIOManager(path_resource=path_resource, extension=".gpkg")


# Definition
definitions = dg.Definitions.merge(
    dg.load_from_defs_folder(project_root=Path(__file__).parent.parent),
    dg.Definitions(
        resources={
            "path_resource": path_resource,
            "overlap_resource": overlap_resource,
            "preference_resource": preference_resource,
            "remove_from_mun_resource": remove_from_mun_resource,
            "affine_resource": affine_resource,
            "switch_resource": switch_resource,
            "gpkg_manager": gpkg_manager,
            "geojson_manager": geojson_manager,
            "points_manager": points_manager,
            "path_geojson_manager": path_geojson_manager,
            "path_gpkg_manager": path_gpkg_manager,
            "csv_manager": csv_manager,
            "json_manager": json_manager,
        },
        jobs=[
            generate_framework_job,
            generate_initial_gcp_job,
            pipeline_1_job,
            pipeline_2_job,
        ],
    ),
)
