from dagster import ConfigurableResource


class PreferenceResource(ConfigurableResource):
    raise_on_deleted_geometries: bool
    mesh_level: int


class PathResource(ConfigurableResource):
    raw_path: str
    out_path: str
    manual_path: str
    ghsl_path: str


class AgebListResource(ConfigurableResource):
    ageb_1990: list[list[str]] | None = None
    ageb_2000: list[list[str]] | None = None
    ageb_2010: list[list[str]] | None = None
    ageb_2020: list[list[str]] | None = None


class AgebDictResource(ConfigurableResource):
    ageb_1990: dict[str, list] | None = None
    ageb_2000: dict[str, list] | None = None
    ageb_2010: dict[str, list] | None = None
    ageb_2020: dict[str, list] | None = None


class AgebNestedDictResource(ConfigurableResource):
    ageb_1990: dict[str, dict[str, list]] | None = None
    ageb_2000: dict[str, dict[str, list]] | None = None
    ageb_2010: dict[str, dict[str, list]] | None = None
    ageb_2020: dict[str, dict[str, list]] | None = None
