import toml

from ageb_alignment.assets import census, metropoli, framework, geometry, georeferencing
from ageb_alignment.assets.census_initial import inegi as census_initial_inegi
from ageb_alignment.assets.census_initial import iter as census_initial_iter
from ageb_alignment.assets.census_initial import scince as census_initial_scince

from ageb_alignment.assets.zones import initial as zones_initial
from ageb_alignment.assets.zones import shaped as zones_shaped


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


zones_initial_assets = load_assets_from_modules([zones_initial], group_name="zones")
zones_shaped_assets = load_assets_from_modules([zones_shaped], group_name="mapshaper")


census_assets = load_assets_from_modules([census], group_name="census")


iter_assets = load_assets_from_modules([census_initial_iter], group_name="iter")
inegi_assets = load_assets_from_modules([census_initial_inegi], group_name="inegi")
scince_assets = load_assets_from_modules([census_initial_scince], group_name="scince")


geometry_assets = load_assets_from_package_module(geometry, group_name="geometry")


georeferencing_assets = load_assets_from_package_module(
    georeferencing, group_name="georeferencing"
)


ageb_assets = load_assets_from_modules(
    [framework.agebs_initial], group_name="agebs_initial"
)
ageb_initial_assets = load_assets_from_modules(
    [framework.agebs_fixed], group_name="agebs"
)
municipality_assets = load_assets_from_modules(
    [framework.municipalities], group_name="municipalities"
)
state_assets = load_assets_from_modules([framework.states], group_name="states")


# Resources
path_resource = PathResource(
    raw_path=EnvVar("RAW_PATH"),
    out_path=EnvVar("OUT_PATH"),
    intermediate_path=EnvVar("INTERMEDIATE_PATH"),
)

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
    + ageb_initial_assets
    + census_assets
    + iter_assets
    + inegi_assets
    + scince_assets
    + georeferencing_assets
    + zones_initial_assets
    + zones_shaped_assets
    + municipality_assets
    + state_assets
    + metropoli_assets,
    resources={
        "path_resource": path_resource,
        "overlap_resource": overlap_resource,
        "remove_from_mun_resource": remove_from_mun_resource,
        "preference_resource": preference_resource,
    },
    jobs=[generate_framework_job, generate_gcp_2000_job, fix_zones_job],
)
