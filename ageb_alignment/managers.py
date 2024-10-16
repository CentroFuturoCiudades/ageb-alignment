from pathlib import Path
from typing import Union

import geopandas as gpd

from ageb_alignment.resources import PathResource
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    ResourceDependency,
)


class FrameworkIOManager(ConfigurableIOManager):
    path_resource: ResourceDependency[PathResource]
    extension: str

    def _get_path(self, context: Union[InputContext, OutputContext]) -> Path:
        out_path = Path(self.path_resource.out_path)
        fpath = out_path / "/".join(context.asset_key.path)

        if context.has_asset_partitions:
            fpath = fpath / context.asset_partition_key

        fpath = fpath.with_suffix(fpath.suffix + self.extension)
        return fpath

    def handle_output(self, context: OutputContext, obj: gpd.GeoDataFrame) -> None:
        out_path = self._get_path(context)
        out_path.parent.mkdir(exist_ok=True, parents=True)
        obj.to_file(out_path)

    def load_input(self, context: InputContext) -> gpd.GeoDataFrame:
        return gpd.read_file(self._get_path(context))
