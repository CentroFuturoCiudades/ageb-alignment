import geopandas as gpd

from ageb_alignment.resources import PathResource
from dagster import asset, AssetIn
from pathlib import Path


def agebs_manual_factory(year: int) -> asset:
    @asset(name=f"agebs_{year}", ins={"agebs": AssetIn(key=f"agebs_{year}_initial")})
    def _asset(path_resource: PathResource, agebs: gpd.GeoDataFrame) -> None:
        root_out_path = Path(path_resource.out_path)

        out_path = root_out_path / "agebs"
        out_path.mkdir(exist_ok=True, parents=True)

        manual_path = Path(path_resource.intermediate_path) / f"replacement/{year}.gpkg"
        if manual_path.exists():
            df_replacement = gpd.read_file(manual_path).set_index("CVEGEO")
            agebs.update(df_replacement)

        agebs.to_file(out_path / f"{year}.gpkg")

    return _asset


agebs_assets = [agebs_manual_factory(year) for year in (1990, 2000, 2010, 2020)]
