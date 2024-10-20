import geopandas as gpd

from ageb_alignment.resources import PathResource
from dagster import asset, AssetExecutionContext, AssetIn
from pathlib import Path


def agebs_manual_factory(year: int) -> asset:
    @asset(
        name=str(year),
        key_prefix=["framework", "agebs"],
        ins={"agebs": AssetIn(key=["agebs_initial", str(year)])},
        io_manager_key="gpkg_manager",
    )
    def _asset(
        context: AssetExecutionContext,
        path_resource: PathResource,
        agebs: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        manual_path = Path(path_resource.intermediate_path) / f"replacement/{year}.gpkg"
        if manual_path.exists():
            df_replacement = gpd.read_file(manual_path).set_index("CVEGEO")
            agebs.update(df_replacement)
            context.log.info(f"Replaced {len(df_replacement)} AGEBs.")
        return agebs

    return _asset


agebs_assets = [agebs_manual_factory(year) for year in (1990, 2000, 2010, 2020)]
