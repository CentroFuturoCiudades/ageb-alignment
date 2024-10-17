from pathlib import Path
from typing import Union

import geopandas as gpd
import pandas as pd

from ageb_alignment.resources import PathResource
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    ResourceDependency,
)


class BaseManager(ConfigurableIOManager):
    path_resource: ResourceDependency[PathResource]
    extension: str

    def _get_path(self, context: Union[InputContext, OutputContext]) -> Path:
        out_path = Path(self.path_resource.out_path)
        fpath = out_path / "/".join(context.asset_key.path)

        if context.has_asset_partitions:
            fpath = fpath / context.asset_partition_key

        fpath = fpath.with_suffix(fpath.suffix + self.extension)
        return fpath


class PathIOManager(BaseManager):
    def handle_output(self, context: OutputContext, obj) -> None:
        pass

    def load_input(self, context: InputContext) -> Path:
        return self._get_path(context)


class DataFrameIOManager(BaseManager):
    def _is_geodataframe(self):
        return self.extension in (".gpkg", ".geojson")

    def handle_output(self, context: OutputContext, obj: gpd.GeoDataFrame) -> None:
        out_path = self._get_path(context)
        out_path.parent.mkdir(exist_ok=True, parents=True)

        if self._is_geodataframe():
            obj.to_file(out_path)
        else:
            obj.to_csv(out_path, index=False)

    def load_input(self, context: InputContext) -> gpd.GeoDataFrame:
        if self._is_geodataframe():
            return gpd.read_file(self._get_path(context))
        else:
            return pd.read_csv(self._get_path(context))
