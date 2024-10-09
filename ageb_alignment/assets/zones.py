import geopandas as gpd

from ageb_alignment.resources import PathResource
from collections import defaultdict
from dagster import asset
from pathlib import Path


remove_from_mun = {
    1990: {
        "01.1.01": ["0100103081640"],
        "02.2.02": ["intersect", "0200101246152", "020010124724A", "0200101247254"],
        "23.1.01": ["2300500010667"],
        "23.2.02": ["intersect"],
        "26.1.01": ["intersect"],
    },
    2000: {
        "02.2.02": ["intersect"],
        "23.1.01": ["intersect"],
        "23.2.02": ["intersect", "2300400111653"],
    },
    2010: {
        "02.2.02": ["intersect"],
        "23.1.01": ["intersect"],
        "23.2.02": ["intersect"],
    },
}


def zone_agebs_factory(year: int):
    @asset(deps=[f"agebs_{year}"], name=f"zone_agebs_{year}")
    def _asset(path_resource: PathResource, municipality_list: dict):
        out_path = Path(path_resource.out_path) / f"zone_agebs/{year}"
        out_path.mkdir(exist_ok=True, parents=True)

        ageb_path = Path(path_resource.out_path) / f"framework/agebs/{year}.gpkg"
        df = (
            gpd.read_file(ageb_path)
            .assign(CVEGEO_MUN=lambda df: df.CVEGEO.str[:5])
            .set_index("CVEGEO")
        )

        for zone, mun_list in municipality_list.items():
            zone_agebs = df[df["CVEGEO_MUN"].isin(mun_list)]
            zone_agebs = zone_agebs[["POBTOT", "geometry"]]
            zone_agebs["geometry"] = zone_agebs["geometry"].make_valid()
            zone_agebs.to_file(out_path / f"{zone}.gpkg")

    return _asset


zone_agebs_assets = [zone_agebs_factory(year) for year in (1990, 2000, 2010, 2020)]
