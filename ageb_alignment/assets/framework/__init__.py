import dagster as dg

from ageb_alignment.assets.framework import agebs, blocks, all


defs = dg.Definitions(
    assets=(
        list(dg.load_assets_from_modules([all, blocks]))
        + list(dg.load_assets_from_package_module(agebs, group_name="framework_agebs"))
    )
)
