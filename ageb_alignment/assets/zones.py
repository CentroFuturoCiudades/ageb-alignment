import geopandas as gpd

from ageb_alignment.resources import AgebDictResource, PathResource
from dagster import asset
from pathlib import Path


def zone_agebs_factory(year: int):
    @asset(deps=[f"agebs_{year}"], name=f"zone_agebs_{year}")
    def _asset(
        path_resource: PathResource,
        remove_from_mun_resource: AgebDictResource,
        municipality_list: dict,
    ):
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
