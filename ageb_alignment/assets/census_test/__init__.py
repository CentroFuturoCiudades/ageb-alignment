import importlib

from dagster import Definitions


iter_1990 = importlib.import_module("ageb_alignment.assets.census_test.iter.1990")
iter_2000 = importlib.import_module("ageb_alignment.assets.census_test.iter.2000")
iter_2010 = importlib.import_module("ageb_alignment.assets.census_test.iter.2010")
iter_2020 = importlib.import_module("ageb_alignment.assets.census_test.iter.2020")

scince_1990 = importlib.import_module("ageb_alignment.assets.census_test.scince.1990")
scince_2000 = importlib.import_module("ageb_alignment.assets.census_test.scince.2000")

inegi_2010 = importlib.import_module("ageb_alignment.assets.census_test.inegi.2010")
inegi_2020 = importlib.import_module("ageb_alignment.assets.census_test.inegi.2020")


defs = Definitions(
    assets=[
        iter_1990.dasset,
        iter_2000.dasset,
        iter_2010.dasset,
        iter_2020.dasset,
        scince_1990.dasset,
        scince_2000.dasset,
        inegi_2010.dasset,
        inegi_2020.dasset,
    ]
)
