from ageb_alignment.assets.geometry import agebs, blocks, loc, mun, state
from dagster import (
    Definitions,
    load_assets_from_modules,
    load_assets_from_package_module,
)

defs = Definitions(
    assets=(
        load_assets_from_package_module(agebs, group_name="geometry_agebs")
        + load_assets_from_modules([loc], group_name="geometry_loc")
        + load_assets_from_modules([mun], group_name="geometry_mun")
        + load_assets_from_modules([state], group_name="geometry_state")
        + load_assets_from_modules([blocks], group_name="geometry_blocks")
    ),
)
