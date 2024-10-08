from dagster import ConfigurableResource


class PathResource(ConfigurableResource):
    raw_path: str
    out_path: str
