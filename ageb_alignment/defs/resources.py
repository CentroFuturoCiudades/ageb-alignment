import dagster as dg


class PreferenceResource(dg.ConfigurableResource):
    raise_on_deleted_geometries: bool
    mesh_level: int


class PathResource(dg.ConfigurableResource):
    data_path: str
    ghsl_path: str


class AgebListResource(dg.ConfigurableResource):
    ageb_1990: list[list[str]] | None = None
    ageb_2000: list[list[str]] | None = None
    ageb_2010: list[list[str]] | None = None
    ageb_2020: list[list[str]] | None = None


class AgebDictResource(dg.ConfigurableResource):
    ageb_1990: dict[str, list] | None = None
    ageb_2000: dict[str, list] | None = None
    ageb_2010: dict[str, list] | None = None
    ageb_2020: dict[str, list] | None = None


class AgebNestedDictResource(dg.ConfigurableResource):
    ageb_1990: dict[str, dict[str, list]] | None = None
    ageb_2000: dict[str, dict[str, list]] | None = None
    ageb_2010: dict[str, dict[str, list]] | None = None
    ageb_2020: dict[str, dict[str, list]] | None = None
