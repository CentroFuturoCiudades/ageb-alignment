import geopandas as gpd
import pandas as pd

from ageb_alignment.assets.framework.agebs import initial as ageb_prep
from ageb_alignment.resources import PathResource
from dagster import asset, graph_asset, AssetExecutionContext, AssetIn
from pathlib import Path


def agebs_manual_factory(year: int) -> asset:
    @asset(
        name=str(year),
        key_prefix=["framework", "agebs"],
        io_manager_key="gpkg_manager",
        ins={
            "ageb": AssetIn(key=["1990", "ageb"]),
            "geometry": AssetIn(key=["geometry", "1990"]),
        },
    )
    def _asset(
        context: AssetExecutionContext,
        path_resource: PathResource,
        ageb: pd.DataFrame,
        geometry: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        prep_func = getattr(ageb_prep, f"prep_agebs_{year}")
        agebs = prep_func(geometry, ageb)

        manual_path = Path(path_resource.intermediate_path) / f"replacement/{year}.gpkg"
        if manual_path.exists():
            df_replacement = gpd.read_file(manual_path).set_index("CVEGEO")
            agebs.update(df_replacement)
            context.log.info(f"Replaced {len(df_replacement)} AGEBs.")
        return agebs

    return _asset


agebs_assets = [agebs_manual_factory(year) for year in (1990, 2000, 2010, 2020)]
