import toml

from ageb_alignment.assets import (
    census,
    framework,
    gcp,
    geometry,
    metropoli,
    translate,
    zones,
)


from ageb_alignment.jobs import (
    generate_framework_job,
    generate_initial_gcp_job,
    fix_zones_job,
)

from ageb_alignment.managers import DataFrameIOManager, PathIOManager

from ageb_alignment.resources import (
    AgebDictResource,
    AgebListResource,
    PathResource,
    PreferenceResource,
)

from dagster import (
    load_assets_from_modules,
    load_assets_from_package_module,
    Definitions,
    EnvVar,
    ExperimentalWarning,
)

# Suppress experimental warnings
import warnings

warnings.filterwarnings("ignore", category=ExperimentalWarning)


# Assets
metropoli_assets = load_assets_from_modules([metropoli], group_name="metropoli")
geometry_assets = load_assets_from_package_module(geometry, group_name="geometry")
translate_assets = load_assets_from_modules([translate], group_name="translate")


# Resources
path_resource = PathResource(
    raw_path=EnvVar("RAW_PATH"),
    out_path=EnvVar("OUT_PATH"),
    intermediate_path=EnvVar("INTERMEDIATE_PATH"),
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
    raise_on_deleted_geometries=preferences["raise_on_deleted_geometries"]
)


# Managers
gpkg_manager = DataFrameIOManager(path_resource=path_resource, extension=".gpkg")
geojson_manager = DataFrameIOManager(path_resource=path_resource, extension=".geojson")
points_manager = DataFrameIOManager(path_resource=path_resource, extension=".points")

path_geojson_manager = PathIOManager(path_resource=path_resource, extension=".geojson")
path_gpkg_manager = PathIOManager(path_resource=path_resource, extension=".gpkg")


# Definition
definitions = Definitions.merge(
    Definitions(
        assets=geometry_assets + metropoli_assets + translate_assets,
        resources={
            "path_resource": path_resource,
            "overlap_resource": overlap_resource,
            "remove_from_mun_resource": remove_from_mun_resource,
            "preference_resource": preference_resource,
            "gpkg_manager": gpkg_manager,
            "geojson_manager": geojson_manager,
            "points_manager": points_manager,
            "path_geojson_manager": path_geojson_manager,
            "path_gpkg_manager": path_gpkg_manager,
        },
        jobs=[generate_framework_job, generate_initial_gcp_job, fix_zones_job],
    ),
    census.defs,
    gcp.defs,
    framework.defs,
    zones.defs,
)
