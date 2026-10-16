from typing import Literal

import geopandas as gpd
import pandas as pd
from cfc_dagster_utils.partitions import zone_partitions
from cfc_dagster_utils.resources import PostgresResource

import dagster as dg


@dg.op(out=dg.Out(io_manager_key="geodataframe_geojson_manager"))
def remove_multipoly(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Removes multipolygons from a GeoDataFrame by exploding them into individual
    polygons.

    Polygons that are part of a multipolygon are split into separate rows, with
    their population (pobtot) divided equally among the parts and a letter suffix
    appended to their cvegeo identifier (e.g., "0101001" becomes "0101001_A",
    "0101001_B", etc.).

    Args:
        df: A GeoDataFrame containing at least "cvegeo", "pobtot", and geometry
            columns.

    Returns:
        A GeoDataFrame where all multipolygons have been replaced by individual
        polygon rows with adjusted cvegeo identifiers and pobtot values.
    """
    counts = df.explode()["cvegeo"].value_counts()
    idx = counts[counts > 1].to_dict()

    nonrepeated = df[~df["cvegeo"].isin(idx.keys())].explode()

    repeated = []
    for cvegeo in idx:
        temp = (
            df[df["cvegeo"] == cvegeo]
            .explode()
            .assign(
                pobtot=lambda df: df["pobtot"] / counts[cvegeo],  # noqa: B023  # ty: ignore[invalid-argument-type]
                suffix=[chr(x + 65) for x in range(counts[cvegeo])],  # ty: ignore[invalid-argument-type]
                cvegeo=lambda df: df["cvegeo"] + "_" + df["suffix"],
            )
            .drop(columns=["suffix"])
        )
        repeated.append(temp)

    return gpd.GeoDataFrame(
        pd.concat([nonrepeated, *repeated], ignore_index=True)
    ).to_crs("EPSG:4326")


def load_agebs_1990_2000_factory(year: Literal[1990, 2000]) -> dg.OpDefinition:
    @dg.op(name=f"load_agebs_{year}")
    def _op(
        context: dg.OpExecutionContext, postgres_resource: PostgresResource
    ) -> gpd.GeoDataFrame:
        with postgres_resource.connect() as conn:
            return gpd.read_postgis(
                f"""
                SELECT cvegeo,
                pob1 AS pobtot,
                geometry
                    FROM geo_unit_{year}_ageb
                INNER JOIN scince_{year}_ageb
                    USING (cvegeo)
                INNER JOIN geometry_{year}_ageb
                    USING (cvegeo)
                WHERE cve_met = %(met_zone)s
                """,  # noqa: S608
                con=conn,
                geom_col="geometry",
                params={"met_zone": context.partition_key},
            )

    return _op


@dg.op
def load_agebs_2010(
    context: dg.OpExecutionContext, postgres_resource: PostgresResource
) -> gpd.GeoDataFrame:
    with postgres_resource.connect() as conn:
        return gpd.read_postgis(
            """
            SELECT
                census_2010_ageb.cvegeo,
                census_2010_ageb.pobtot,
                census_2010_ageb.geometry
            FROM census_2010_ageb
            WHERE census_2010_ageb.cve_met = %(met_zone)s
            """,
            con=conn,
            geom_col="geometry",
            params={"met_zone": context.partition_key},
        )


@dg.op
def load_agebs_2020(
    context: dg.OpExecutionContext, postgres_resource: PostgresResource
) -> gpd.GeoDataFrame:
    with postgres_resource.connect() as conn:
        return gpd.read_postgis(
            """
            SELECT
                census_2020_ageb.cvegeo,
                census_2020_ageb.pobtot,
                census_2020_ageb.geometry
            FROM census_2020_ageb
            INNER JOIN census_2020_loc
                ON census_2020_ageb.cve_loc = census_2020_loc.cvegeo
            INNER JOIN census_2020_mun
                ON census_2020_loc.cve_mun = census_2020_mun.cvegeo
            WHERE census_2020_mun.cve_met = %(met_zone)s
            """,
            con=conn,
            geom_col="geometry",
            params={"met_zone": context.partition_key},
        )


load_agebs_op_map = {
    1990: load_agebs_1990_2000_factory(1990),
    2000: load_agebs_1990_2000_factory(2000),
    2010: load_agebs_2010,
    2020: load_agebs_2020,
}


def zone_agebs_factory(year: Literal[1990, 2000, 2010, 2020]) -> dg.AssetsDefinition:
    @dg.graph_asset(
        key=["zone_agebs", "initial", str(year)],
        partitions_def=zone_partitions,
        group_name="initial",
    )
    def _asset() -> gpd.GeoDataFrame:
        return remove_multipoly(load_agebs_op_map[year]())

    return _asset


zone_agebs_assets = [zone_agebs_factory(year) for year in (1990, 2000, 2010, 2020)]
