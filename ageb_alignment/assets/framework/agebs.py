from ageb_alignment.resources import PathResource
from ageb_alignment.types import GeometryTuple, CensusTuple
from dagster import asset, AssetIn
from pathlib import Path


@asset(ins={"census_1990": AssetIn(key=["census", "1990"])})
def agebs_1990(
    path_resource: PathResource, geometry_1990: GeometryTuple, census_1990: CensusTuple
) -> None:
    out_path = Path(path_resource.out_path) / "framework/agebs"
    out_path.mkdir(exist_ok=True, parents=True)

    merged = (
        geometry_1990.ageb.join(census_1990.ageb, how="left")
        .fillna(0)
        .assign(POBTOT=lambda df: df.POBTOT.astype(int))
        .explode()
        .dissolve(by="CVEGEO")
    )
    merged.to_file(out_path / "1990.gpkg")


@asset(ins={"census_2000": AssetIn(key=["census", "2000"])})
def agebs_2000(
    path_resource: PathResource, geometry_2000: GeometryTuple, census_2000: CensusTuple
) -> None:
    out_path = Path(path_resource.out_path) / "framework/agebs"
    out_path.mkdir(exist_ok=True, parents=True)

    merged = geometry_2000.ageb.join(census_2000.ageb, how="left").sort_index()
    merged.to_file(out_path / "2000.gpkg")


@asset(ins={"census_2010": AssetIn(key=["census", "2010"])})
def agebs_2010(
    path_resource: PathResource, geometry_2010: GeometryTuple, census_2010: CensusTuple
) -> None:
    out_path = Path(path_resource.out_path) / "framework/agebs"
    out_path.mkdir(exist_ok=True, parents=True)

    merged = (
        geometry_2010.ageb.drop(
            columns=[
                "CODIGO",
                "GEOGRAFICO",
                "FECHAACT",
                "GEOMETRIA",
                "INSTITUCIO",
                "OID",
            ]
        )
        .assign(
            CVE_ENT=lambda df: df.index.str[0:2].astype(int),
            CVE_MUN=lambda df: df.index.str[2:5].astype(int),
            CVE_LOC=lambda df: df.index.str[5:9].astype(int),
            CVE_AGEB=lambda df: df.index.str[9:],
        )
        .sort_index()
        .join(census_2010.ageb, how="left")
        .assign(POBTOT=lambda df: df.POBTOT.fillna(0).astype(int))
    )
    merged.to_file(out_path / "2010.gpkg")


@asset(ins={"census_2020": AssetIn(key=["census", "2020"])})
def agebs_2020(
    path_resource: PathResource, geometry_2020: GeometryTuple, census_2020: CensusTuple
) -> None:
    out_path = Path(path_resource.out_path) / "framework/agebs"
    out_path.mkdir(exist_ok=True, parents=True)

    merged = (
        geometry_2020.ageb.drop(columns="Ambito")
        .assign(
            CVE_ENT=lambda df: df.CVE_ENT.astype(int),
            CVE_MUN=lambda df: df.CVE_MUN.astype(int),
            CVE_LOC=lambda df: df.CVE_LOC.astype(int),
        )
        .sort_index()
        .join(census_2020.ageb, how="left")
    )
    merged.to_file(out_path / "2020.gpkg")
