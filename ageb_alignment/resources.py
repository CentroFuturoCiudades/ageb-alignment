from dagster import ConfigurableResource
from typing import Optional


class PathResource(ConfigurableResource):
    raw_path: str
    out_path: str


class AgebEnumResource(ConfigurableResource):
    ageb_1990: Optional[list] = None
    ageb_2000: Optional[list] = None
    ageb_2010: Optional[list] = None
    ageb_2020: Optional[list] = None
