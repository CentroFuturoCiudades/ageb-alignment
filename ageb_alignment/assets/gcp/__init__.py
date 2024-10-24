from ageb_alignment.assets.gcp import initial
from dagster import Definitions, load_assets_from_modules


defs = Definitions(
    assets=(load_assets_from_modules([initial], group_name="gcp_initial"))
)
