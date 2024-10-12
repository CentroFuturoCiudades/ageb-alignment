import geopandas as gpd
import numpy as np
import pandas as pd

from ageb_alignment.resources import PathResource
from dagster import asset, AssetExecutionContext
from pathlib import Path


def get_tcma(x0, x1, num_years):
    """Calculates the mean anual growth rate.

    Parameters
    ----------
    x0 : numeric or array like
        Values at the starting year.
    x1 : numeric or array like
        Values at the end of the interval.
    num_years : int
        Interval lenght in years.

    Returns
    -------
    int or array like
        The mean anual growth rate.
    """
    return ((x1 / x0) ** (1 / num_years) - 1) * 100


@asset
def metropoli_2020(path_resource: PathResource) -> gpd.GeoDataFrame:
    metropoli_path = Path(path_resource.raw_path) / "metropoli/2020"
    metropoli_muns_gdf = (
        gpd.read_file(metropoli_path)
        .set_index(["CVE_MET", "CVEGEO"])
        .to_crs("ESRI:102008")
        .drop(columns="NOM_MET")
    )
    return metropoli_muns_gdf


@asset
def metropoli_table(path_resource: PathResource) -> pd.DataFrame:
    sheet_path = Path(path_resource.raw_path) / "metropoli/Cuadros_MM2020.xlsx"

    sheet_a = (
        pd.read_excel(sheet_path, sheet_name="Cuadro A_MUN")
        .drop(
            columns=[
                "Tipo de metrópoli",
                "Nombre de la entidad",
                "Clave de la entidad",
                "Clave de municipio",
                "Nombre del municipio",
                "Tasa de crecimiento medio anual 1990-2000",
                "Tasa de crecimiento medio anual  2000-2010",
                "Tasa de crecimiento medio anual  2010-2020",
                "Superficie km2",
            ]
        )
        .rename(
            columns={
                "Nombre de la metrópoli": "NOM_MET",
                "Clave de metrópoli": "CVE_MET",
                "Clave compuesta del municipio": "CVEGEO",
                "Población 1990": "POB_TOT_1990",
                "Población 2000": "POB_TOT_2000",
                "Población 2010": "POB_TOT_2010",
                "Población 2020": "POB_TOT_2020",
                "Densidad media urbana": "PWDENSITY_URB_2020",
            }
        )
        .assign(
            CVEGEO=lambda x: x.CVEGEO.astype(str).str.pad(5, side="left", fillchar="0"),
            TCMA_TOT_1990_2000=lambda x: get_tcma(
                x["POB_TOT_1990"], x["POB_TOT_2000"], 10
            ).replace(np.inf, np.nan),
            TCMA_TOT_2000_2010=lambda x: get_tcma(
                x["POB_TOT_2000"], x["POB_TOT_2010"], 10
            ).replace(np.inf, np.nan),
            TCMA_TOT_2010_2020=lambda x: get_tcma(
                x["POB_TOT_2010"], x["POB_TOT_2020"], 10
            ).replace(np.inf, np.nan),
        )
        .set_index(["CVE_MET", "CVEGEO"])
    )

    sheet_b = (
        pd.read_excel(sheet_path, sheet_name="Cuadro B_MUN")
        .drop(
            columns=[
                "Tipo de metrópoli",
                "Nombre de la metrópoli",
                "Nombre de la entidad",
                "Clave de la entidad",
                "Clave de municipio",
                "Nombre del municipio",
            ]
        )
        .rename(
            columns={
                "Clave de metrópoli": "CVE_MET",
                "Clave compuesta del municipio": "CVEGEO",
            }
        )
        .assign(
            CVEGEO=lambda x: x.CVEGEO.astype(str).str.pad(5, side="left", fillchar="0")
        )
        .set_index(["CVE_MET", "CVEGEO"])
        .replace("●", "1")
        .fillna(0)
        .astype(int)
        .assign(
            CENTRAL=lambda x: np.logical_or(
                x["Municipios centrales. Conurbación física"],
                x["Municipios centrales. Integración funcional"],
            ).astype(int),
            FUNCTIONAL=lambda x: np.logical_or(
                x["Municipios centrales. Integración funcional"],
                x["Municipios exteriores. Integración funcional"],
            ).astype(int),
            CENTRAL_MIN_POB=lambda x: (
                200
                * x[
                    "Municipios centrales. Localidad o conurbación de 200 mil o "
                    "más habitantes o capital estatal"
                ]
                + 100
                * x[
                    "Municipios centrales. Localidad o conurbación de 100 mil o "
                    "más habitantes"
                ]
                + 50 * x["Municipios exteriores. Continuidad geográfica"]
            ),
        )
        .drop(
            columns=[
                "Municipios centrales. Conurbación física",
                "Municipios centrales. Integración funcional",
                "Municipios exteriores. Integración funcional",
                "Municipios exteriores. Continuidad geográfica",
                "Municipios centrales. Localidad o conurbación de 200 mil o "
                "más habitantes o capital estatal",
                "Municipios centrales. Localidad o conurbación de 100 mil o "
                "más habitantes",
                "Municipios centrales. Localidad o conurbación de 50 mil o "
                "más habitantes",
            ]
        )
    )

    metropoli_muns_gdf = pd.concat([sheet_a, sheet_b], axis=1)
    return metropoli_muns_gdf


@asset
def metropoli_list(
    context: AssetExecutionContext,
    metropoli_2020: gpd.GeoDataFrame,
    metropoli_table: pd.DataFrame,
) -> dict:
    df = (
        pd.concat([metropoli_2020, metropoli_table], axis=1)
        .assign(AREA_TOT=lambda x: x.area / 1e6)
        .rename_axis(index={"CVEGEO": "CVE_MUN"})
        .query("TIPO_MET != 'Zona conurbada'")
        .drop("23.2.03")
        .sort_index()
    )

    zones_mun_dict = {}
    unique_zones = set()
    for zone, mun in df.index:
        if zone in zones_mun_dict:
            zones_mun_dict[zone].append(mun)
        else:
            zones_mun_dict[zone] = [mun]

        unique_zones.add(zone)

    context.instance.add_dynamic_partitions("zone", list(unique_zones))
    return zones_mun_dict
