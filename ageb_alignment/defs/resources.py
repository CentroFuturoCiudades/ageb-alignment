import sqlalchemy
from pydantic import PrivateAttr

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


class PostGISResource(dg.ConfigurableResource):
    host: str
    port: str
    user: str
    password: str
    db: str

    _engine: sqlalchemy.engine.Engine = PrivateAttr()

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:  # noqa: ARG002
        self._engine = sqlalchemy.create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}",
        )

    def get_connection(self) -> sqlalchemy.engine.Connection:
        return self._engine.connect()
