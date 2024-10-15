import geopandas as gpd

from ageb_alignment.resources import PathResource
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    ResourceDependency,
)
from pathlib import Path


class FrameworkIOManager(ConfigurableIOManager):
    path_resource: ResourceDependency[PathResource]

    def _get_path(self, context) -> str:
        out_path = Path(self.path_resource.out_path)
        framework_path = out_path / "/".join(context.asset_key.path)
        framework_path = framework_path.with_suffix(".gpkg")
        return str(framework_path)

    def handle_output(self, context: OutputContext, obj: gpd.GeoDataFrame):
        out_path = Path(self._get_path(context))
        out_path.parent.mkdir(exist_ok=True, parents=True)
        obj.to_file(out_path)

    def load_input(self, context: InputContext):
        return gpd.read_file(self._get_path(context))
