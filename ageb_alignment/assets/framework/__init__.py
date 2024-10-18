from ageb_alignment.assets.framework import municipalities, states

import ageb_alignment.assets.framework.agebs.initial as agebs_initial
import ageb_alignment.assets.framework.agebs.fixed as agebs_fixed

from dagster import load_assets_from_modules, Definitions


defs = Definitions(
    assets=(
        load_assets_from_modules(
            [municipalities], group_name="framework_municipalities"
        )
        + load_assets_from_modules([states], group_name="framework_states")
        + load_assets_from_modules(
            [agebs_initial], group_name="framework_agebs_initial"
        )
        + load_assets_from_modules([agebs_fixed], group_name="framework_agebs")
    )
)
