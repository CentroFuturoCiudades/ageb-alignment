import json
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

import dagster as dg
from ageb_alignment.defs.resources import PathResource


class BaseManager(dg.ConfigurableIOManager):
    path_resource: dg.ResourceDependency[PathResource]
    extension: str

    def _get_path(
        self,
        context: dg.InputContext | dg.OutputContext,
    ) -> Path | dict[str, Path]:
        out_path = Path(self.path_resource.data_path) / "final"
        fpath = out_path / "/".join(context.asset_key.path)

        if context.has_asset_partitions:
            try:
                final_path = fpath / context.asset_partition_key
                final_path = final_path.with_suffix(final_path.suffix + self.extension)
            except Exception:
                final_path = {}
                for key in context.asset_partition_keys:
                    temp_path = fpath / key
                    temp_path = temp_path.with_suffix(temp_path.suffix + self.extension)
                    final_path[key] = temp_path
        else:
            final_path = fpath.with_suffix(fpath.suffix + self.extension)

        return final_path


class PathIOManager(BaseManager):
    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:  # noqa: ANN401
        raise NotImplementedError

    def load_input(self, context: dg.InputContext) -> Path:
        path = self._get_path(context)

        if not isinstance(path, Path):
            err = f"PathIOManager: {path} is not a Path"
            raise TypeError(err)

        if not path.exists():
            err = f"PathIOManager: {path} does not exist"
            raise FileNotFoundError(err)

        return path


class DataFrameIOManager(BaseManager):
    with_index: bool = True

    def _is_geodataframe(self) -> bool:
        return self.extension in (".gpkg", ".geojson")

    def handle_output(self, context: dg.OutputContext, obj: gpd.GeoDataFrame) -> None:
        out_path = self._get_path(context)

        if not isinstance(out_path, Path):
            err = f"DataFrameIOManager: {out_path} is not a Path"
            raise TypeError(err)

        out_path.parent.mkdir(exist_ok=True, parents=True)

        if self._is_geodataframe():
            obj.to_file(out_path, mode="w")
        else:
            obj.to_csv(out_path, index=self.with_index)

    def load_input(
        self,
        context: dg.InputContext,
    ) -> (
        gpd.GeoDataFrame
        | pd.DataFrame
        | dict[str, gpd.GeoDataFrame]
        | dict[str, pd.DataFrame]
    ):
        path = self._get_path(context)
        if isinstance(path, Path):
            if self._is_geodataframe():
                return gpd.read_file(path)
            return pd.read_csv(path)
        if isinstance(path, dict):
            out_dict = {}
            for key, fpath in path.items():
                if self._is_geodataframe():
                    out_dict[key] = gpd.read_file(fpath)
                else:
                    out_dict[key] = pd.read_csv(fpath)
            return out_dict
        err = f"PathIOManager: {path} is not a Path or dict"
        raise TypeError(err)


class JSONIOManager(BaseManager):
    def handle_output(self, context: dg.OutputContext, obj: dict) -> None:
        out_path = self._get_path(context)

        if not isinstance(out_path, Path):
            err = f"JSONIOManager: {out_path} is not a Path"
            raise TypeError(err)

        out_path.parent.mkdir(exist_ok=True, parents=True)
        with out_path.open("w") as f:
            json.dump(obj, f)

    def load_input(self, context: dg.InputContext) -> dict:
        path = self._get_path(context)
        if isinstance(path, Path):
            with path.open() as f:
                return json.load(f)
        elif isinstance(path, dict):
            out_dict = {}
            for key, fpath in path.items():
                with fpath.open() as f:
                    out_dict[key] = json.load(f)
            return out_dict
        else:
            err = f"PathIOManager: {path} is not a Path or dict"
            raise TypeError(err)
