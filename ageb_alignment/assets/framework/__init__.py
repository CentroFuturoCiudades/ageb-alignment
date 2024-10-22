from ageb_alignment.assets.framework import municipalities, states

import ageb_alignment.assets.framework.agebs as agebs

from dagster import (
    load_assets_from_modules,
    load_assets_from_package_module,
    Definitions,
)


defs = Definitions(
    assets=(
        load_assets_from_modules(
            [municipalities], group_name="framework_municipalities"
        )
        + load_assets_from_modules([states], group_name="framework_states")
        + load_assets_from_package_module(agebs, group_name="framework_agebs")
    )
)
