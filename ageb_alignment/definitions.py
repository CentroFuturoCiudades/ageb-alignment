from ageb_alignment.assets import (
    census,
    framework,
    geometry,
    georeferencing,
    mapshaper,
    metropoli,
    met_zones,
    zones,
)

from ageb_alignment.resources import PathResource
from dagster import (
    load_assets_from_modules,
    load_assets_from_package_module,
    Definitions,
    EnvVar,
)


metropoli_assets = load_assets_from_modules([metropoli], group_name="metropoli")
met_zones_assets = load_assets_from_modules([met_zones], group_name="met_zones")
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

defs = Definitions(
    assets=geometry_assets
    + census_assets
    + ageb_assets
    + municipality_assets
    + state_assets
    + metropoli_assets
    + zones_assets,
    resources={
        "path_resource": PathResource(
            raw_path=EnvVar("RAW_PATH"), out_path=EnvVar("OUT_PATH")
        )
    },
)