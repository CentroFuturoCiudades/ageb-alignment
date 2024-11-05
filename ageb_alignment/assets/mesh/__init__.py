from ageb_alignment.assets.mesh import differences, reproject
from dagster import load_assets_from_modules, Definitions

defs = Definitions(
    assets=(load_assets_from_modules([differences, reproject], group_name="mesh"))
)
