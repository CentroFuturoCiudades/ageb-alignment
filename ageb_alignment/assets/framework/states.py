from ageb_alignment.resources import PathResource
from ageb_alignment.types import CensusTuple, GeometryTuple
from dagster import asset
from pathlib import Path


@asset
def states_2000(path_resource: PathResource, geometry_2000: GeometryTuple) -> None:
    out_path = Path(path_resource.out_path) / "framework/states"
    out_path.mkdir(exist_ok=True, parents=True)

    merged = (
        geometry_2000.ent.drop(columns="OID")
        .assign(CVE_ENT=lambda df: df.CVE_ENT.astype(int))
        .set_index("CVE_ENT")
        .sort_index()
    )
    merged.to_file(out_path / "2000.gpkg")


@asset
def states_2010(
    path_resource: PathResource, geometry_2010: GeometryTuple, census_2010: CensusTuple
) -> None:
    out_path = Path(path_resource.out_path) / "framework/states"
    out_path.mkdir(exist_ok=True, parents=True)

    merged = (
        geometry_2010.ent.drop(columns=["OID", "NOM_ENT"])
        .assign(CVE_ENT=lambda df: df.CVE_ENT.astype(int))
        .set_index("CVE_ENT")
        .sort_index()
        .join(census_2010.ent)
    )
    merged.to_file(out_path / "2010.gpkg")


@asset
def states_2020(
    path_resource: PathResource, geometry_2020: GeometryTuple, census_2020: CensusTuple
) -> None:
    out_path = Path(path_resource.out_path) / "framework/states"
    out_path.mkdir(exist_ok=True, parents=True)

    merged = (
        geometry_2020.ent.drop(columns=["NOMGEO", "CVEGEO"])
        .assign(CVE_ENT=lambda df: df.CVE_ENT.astype(int))
        .set_index("CVE_ENT")
        .sort_index()
        .join(census_2020.ent)
    )
    merged.to_file(out_path / "2020.gpkg")
