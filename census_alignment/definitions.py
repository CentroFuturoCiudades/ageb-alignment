from census_alignment.assets import filter, georeferencing, mapshaper
from census_alignment.assets.loading import agebs, census, metropoli

from census_alignment.resources import PathResource
from dagster import load_assets_from_modules, load_assets_from_package_module, Definitions, EnvVar


loading_agebs_assets = load_assets_from_modules([agebs], group_name="loading_agebs")
loading_census_assets = load_assets_from_modules([census], group_name="loading_census")
loading_metropoli_assets = load_assets_from_modules([metropoli], group_name="loading_metropoli")

filtering_assets = load_assets_from_modules([filter], group_name="filtering")

mapshaper_assets = load_assets_from_modules([mapshaper], group_name="mapshaper")

georeferencing_assets = load_assets_from_package_module(georeferencing, group_name="georeferencing")

defs = Definitions(
    assets=loading_agebs_assets + loading_census_assets + loading_metropoli_assets + filtering_assets + georeferencing_assets + mapshaper_assets,
    resources={
        "path_resource": PathResource(
            raw_path=EnvVar("RAW_PATH"),
            out_path=EnvVar("OUT_PATH")
        )
    }
)