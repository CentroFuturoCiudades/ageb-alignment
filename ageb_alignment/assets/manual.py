from ageb_alignment.resources import PathResource
from dagster import asset, AssetExecutionContext
from pathlib import Path


def manual_corrections_factory(context: AssetExecutionContext, year: int) -> asset:
    @asset(name=f"manual_corrections_{year}", deps=[f"zone_agebs_fixed_{year}"])
    def _asset(path_resource: PathResource):
        root_out_path = Path(path_resource.out_path)
        agebs_fixed_path = root_out_path / f"zone_agebs_fixed/{year}"
        agebs_manual_path = root_out_path / f"zone_agebs_manual/{year}"
