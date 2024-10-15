from ageb_alignment.resources import PathResource
from dagster import asset, AssetExecutionContext
from pathlib import Path


def manual_corrections_factory(context: AssetExecutionContext, year: int) -> asset:
    @asset(name=f"manual_corrections_{year}", deps=["zone_agebs", "shaped", str(year)])
    def _asset(path_resource: PathResource):
        root_out_path = Path(path_resource.out_path)
        agebs_fixed_path = root_out_path / f"zone_agebs_shaped/{year}"

        agebs_manual_path = root_out_path / f"zone_agebs_manual/{year}"
        agebs_manual_path.mkdir(exist_ok=True, parents=True)
