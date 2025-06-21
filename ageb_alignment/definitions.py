import os
import toml

from ageb_alignment.assets import (
    built,
    census,
    differences,
    framework,
    gcp,
    geometry,
    mesh,
    metropoli,
    translate,
    zones,
)


from ageb_alignment.jobs import (
    generate_framework_job,
    generate_initial_gcp_job,
    pipeline_1_job,
    pipeline_2_job,
)

from ageb_alignment.managers import DataFrameIOManager, JSONIOManager, PathIOManager

from ageb_alignment.resources import (
    AgebDictResource,
    AgebNestedDictResource,
    AgebListResource,
    PathResource,
    PreferenceResource,
)

from dagster import (
    load_assets_from_modules,
    load_assets_from_package_module,
    Definitions,
    EnvVar,
)


# Assets
differences_assets = load_assets_from_modules([differences], group_name="differences")
metropoli_assets = load_assets_from_modules([metropoli], group_name="metropoli")
gcp_assets = load_assets_from_modules([gcp], group_name="gcp")
translate_assets = load_assets_from_package_module(translate, group_name="translate")
built_assets = load_assets_from_modules([built], group_name="built")


# Resources
path_resource = PathResource(
    raw_path=EnvVar("RAW_PATH"),
    manual_path=EnvVar("INTERMEDIATE_PATH"),
    out_path=EnvVar("OUT_PATH"),
    ghsl_path=EnvVar("GHSL_GLOBAL_PATH"),
)

with open("./configs/overlaps.toml", "r", encoding="utf8") as f:
    overlap_list = toml.load(f)
overlap_list = {f"ageb_{key}": value for key, value in overlap_list.items()}
overlap_resource = AgebListResource(**overlap_list)

with open("./configs/remove_from_mun.toml", encoding="utf8") as f:
    remove_from_mun_list = toml.load(f)
remove_from_mun_list = {
    f"ageb_{key}": value for key, value in remove_from_mun_list.items()
}
remove_from_mun_resource = AgebDictResource(**remove_from_mun_list)

with open("./configs/preferences.toml", "r", encoding="utf8") as f:
    preferences = toml.load(f)
preference_resource = PreferenceResource(
    raise_on_deleted_geometries=preferences["raise_on_deleted_geometries"],
    mesh_level=preferences["mesh_level"],
)

with open("./configs/switches.toml", encoding="utf8") as f:
    switch_list = toml.load(f)
switch_list = {f"ageb_{key}": value for key, value in switch_list.items()}
switch_resource = AgebNestedDictResource(**switch_list)


with open("./configs/affine.toml", encoding="utf8") as f:
    rigid_list = toml.load(f)
rigid_list = {f"ageb_{key}": value for key, value in rigid_list.items()}
affine_resource = AgebDictResource(**rigid_list)


# Managers
gpkg_manager = DataFrameIOManager(path_resource=path_resource, extension=".gpkg")
geojson_manager = DataFrameIOManager(path_resource=path_resource, extension=".geojson")
points_manager = DataFrameIOManager(path_resource=path_resource, extension=".points", with_index=False)
csv_manager = DataFrameIOManager(path_resource=path_resource, extension=".csv")
json_manager = JSONIOManager(path_resource=path_resource, extension=".json")

path_geojson_manager = PathIOManager(path_resource=path_resource, extension=".geojson")
path_gpkg_manager = PathIOManager(path_resource=path_resource, extension=".gpkg")


# Definition
definitions = Definitions.merge(
    Definitions(
        assets=metropoli_assets
        + translate_assets
        + gcp_assets
        + differences_assets
        + built_assets,
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
    census.defs,
    framework.defs,
    geometry.defs,
    mesh.defs,
    zones.defs,
)
