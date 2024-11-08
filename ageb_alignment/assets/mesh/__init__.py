from ageb_alignment.assets.mesh import merge, reproject
from dagster import load_assets_from_modules, Definitions

defs = Definitions(
    assets=(load_assets_from_modules([merge, reproject], group_name="mesh"))
)
