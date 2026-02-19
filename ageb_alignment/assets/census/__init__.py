import importlib

import dagster as dg
from ageb_alignment.assets.census import inegi, iter, scince

defs = dg.Definitions(
    assets=list(dg.load_assets_from_modules([inegi, iter, scince])),
)
