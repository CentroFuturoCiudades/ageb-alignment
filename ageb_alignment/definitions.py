import toml

from ageb_alignment.assets import (
    census,
    framework,
    geometry,
    georeferencing,
    mapshaper,
    metropoli,
    zones,
)

from ageb_alignment.jobs import (
    generate_framework_job,
    generate_gcp_2000_job,
    fix_zones_job,
)

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
mapshaper_assets = load_assets_from_modules([mapshaper], group_name="mapshaper")

census_assets = load_assets_from_package_module(census, group_name="census")

geometry_assets = load_assets_from_package_module(geometry, group_name="geometry")

georeferencing_assets = load_assets_from_package_module(
    georeferencing, group_name="georeferencing"
)

ageb_assets = load_assets_from_modules([framework.agebs], group_name="agebs")
municipality_assets = load_assets_from_modules(
    [framework.municipalities], group_name="municipalities"
)
state_assets = load_assets_from_modules([framework.states], group_name="states")

zones_assets = load_assets_from_modules([zones], group_name="zones")


# Resources
path_resource = PathResource(raw_path=EnvVar("RAW_PATH"), out_path=EnvVar("OUT_PATH"))

with open("./config.toml", "r", encoding="utf8") as f:
    config = toml.load(f)

overlap_list = {}
for year, agebs in config["overlaps"].items():
    overlap_list[f"ageb_{year}"] = agebs
overlap_resource = AgebListResource(**overlap_list)

remove_from_mun_list = {}
for year, agebs in config["remove_from_mun"].items():
    remove_from_mun_list[f"ageb_{year}"] = agebs
remove_from_mun_resource = AgebDictResource(**remove_from_mun_list)

preference_resource = PreferenceResource(
    raise_on_deleted_geometries=config["preferences"]["raise_on_deleted_geometries"]
)


# Definition
defs = Definitions(
    assets=geometry_assets
    + ageb_assets
    + census_assets
    + georeferencing_assets
    + mapshaper_assets
    + municipality_assets
    + state_assets
    + metropoli_assets
    + zones_assets,
    resources={
        "path_resource": path_resource,
        "overlap_resource": overlap_resource,
        "remove_from_mun_resource": remove_from_mun_resource,
        "preference_resource": preference_resource,
    },
    jobs=[generate_framework_job, generate_gcp_2000_job, fix_zones_job],
)
