import geopandas as gpd
import pandas as pd

from census_alignment.resources import PathResource
from dagster import asset
from pathlib import Path


@asset
def load_metropoli(path_resource: PathResource) -> pd.DataFrame:
    path = Path(path_resource.raw_path) / "metropoli/mpios_en_metropoli.shp"
    df = gpd.read_file(path, engine="pyogrio")
    df = df[df["TIPOMET"].isin(["Zona metropolitana", "Metrópoli municipal"])]
    df = df[["NOMGEO_COR", "CVEGEO"]]
    df = df.rename(columns={"CVEGEO": "stub", "NOMGEO_COR": "METROPOLI"})
    df["METROPOLI"] = df["METROPOLI"].str.casefold()
    return df