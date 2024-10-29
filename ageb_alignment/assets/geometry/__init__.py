import importlib

from dagster import load_assets_from_modules, Definitions

geometry_1990 = importlib.import_module("ageb_alignment.assets.geometry.1990")
geometry_2000 = importlib.import_module("ageb_alignment.assets.geometry.2000")
geometry_2010 = importlib.import_module("ageb_alignment.assets.geometry.2010")
geometry_2020 = importlib.import_module("ageb_alignment.assets.geometry.2020")


defs = Definitions(
    assets=(
        load_assets_from_modules([geometry_1990], group_name="geometry_1990")
        + load_assets_from_modules([geometry_2000], group_name="geometry_2000")
        + load_assets_from_modules([geometry_2010], group_name="geometry_2010")
        + load_assets_from_modules([geometry_2020], group_name="geometry_2020")
    )
)
