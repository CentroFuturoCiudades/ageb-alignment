from ageb_alignment.assets.geometry import agebs, mun, state
from dagster import (
    load_assets_from_modules,
    load_assets_from_package_module,
    Definitions,
)

defs = Definitions(
    assets=(
        load_assets_from_package_module(agebs, group_name="geometry_agebs")
        + load_assets_from_modules([mun], group_name="geometry_mun")
        + load_assets_from_modules([state], group_name="geometry_state")
    )
)
