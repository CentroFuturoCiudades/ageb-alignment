from ageb_alignment.assets.mesh import merge, reproject
from dagster import Definitions, load_assets_from_modules

defs = Definitions(
    assets=(load_assets_from_modules([merge, reproject], group_name="mesh")),
)
