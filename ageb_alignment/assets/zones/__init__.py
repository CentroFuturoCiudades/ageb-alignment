from ageb_alignment.assets.zones import extended, initial, replaced, shaped, switched
from dagster import Definitions, load_assets_from_modules


defs = Definitions(
    assets=(
        load_assets_from_modules([extended], group_name="zones_extended")
        + load_assets_from_modules([initial], group_name="zones_initial")
        + load_assets_from_modules([replaced], group_name="zones_replaced")
        + load_assets_from_modules([shaped], group_name="zones_shaped")
        + load_assets_from_modules([switched], group_name="zones_switched")
    )
)
