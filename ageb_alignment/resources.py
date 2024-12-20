from dagster import ConfigurableResource
from typing import Optional


class PreferenceResource(ConfigurableResource):
    raise_on_deleted_geometries: bool
    mesh_level: int


class PathResource(ConfigurableResource):
    raw_path: str
    out_path: str
    manual_path: str
    ghsl_path: str


class AgebListResource(ConfigurableResource):
    ageb_1990: Optional[list[list[str]]] = None
    ageb_2000: Optional[list[list[str]]] = None
    ageb_2010: Optional[list[list[str]]] = None
    ageb_2020: Optional[list[list[str]]] = None


class AgebDictResource(ConfigurableResource):
    ageb_1990: Optional[dict[str, list]] = None
    ageb_2000: Optional[dict[str, list]] = None
    ageb_2010: Optional[dict[str, list]] = None
    ageb_2020: Optional[dict[str, list]] = None


class AgebNestedDictResource(ConfigurableResource):
    ageb_1990: Optional[dict[str, dict[str, list]]] = None
    ageb_2000: Optional[dict[str, dict[str, list]]] = None
    ageb_2010: Optional[dict[str, dict[str, list]]] = None
    ageb_2020: Optional[dict[str, dict[str, list]]] = None
