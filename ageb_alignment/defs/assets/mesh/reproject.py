import geopandas as gpd
from cfc_dagster_utils.partitions import zone_partitions
from cfc_dagster_utils.resources import PostgresResource

import dagster as dg
from ageb_alignment.defs.assets.mesh.validation import validate_cell_codes

MESH_QUERY = """
SELECT DISTINCT mesh.*
    FROM geo_unit_2020_mun AS unit
JOIN geometry_2020_mun AS municipality USING (cvegeo)
JOIN mesh_level_9 AS mesh
    ON ST_Intersects(mesh.geometry, municipality.geometry)
WHERE unit.cve_met = %(zone)s
"""


@dg.asset(
    key="mesh",
    partitions_def=zone_partitions,
    io_manager_key="geodataframe_manager",
    group_name="mesh",
)
def load_mesh(
    context: dg.AssetExecutionContext, postgres_resource: PostgresResource
) -> gpd.GeoDataFrame:
    with postgres_resource.connect() as conn:
        mesh = gpd.read_postgis(
            MESH_QUERY,
            conn,
            params={"zone": context.partition_key},
            geom_col="geometry",
        )
    validate_cell_codes(mesh, source=f"mesh partition {context.partition_key}")
    return mesh


def reprojected_factory(year: int) -> dg.AssetsDefinition:
    if year in (1990, 2000):
        ageb_asset = dg.AssetIn(key=["zone_agebs", "translated", str(year)])
    elif year in (2010, 2020):
        ageb_asset = dg.AssetIn(key=["zone_agebs", "shaped", str(year)])

    @dg.asset(
        key=["reprojected", "base", str(year)],
        ins={"agebs": ageb_asset},
        partitions_def=zone_partitions,
        io_manager_key="geodataframe_manager",
        group_name="reprojected",
    )
    def _asset(
        mesh: gpd.GeoDataFrame,
        agebs: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        validate_cell_codes(mesh, source=f"mesh input for reprojection {year}")
        crs = agebs.crs
        if crs is None:
            err = "CRS of agebs GeoDataFrame is not defined. Cannot reproject to mesh."
            raise ValueError(err)

        mesh = mesh.to_crs(crs)

        agebs = agebs.copy()
        agebs["ageb_area"] = agebs.area
        intersection: gpd.GeoDataFrame = mesh.overlay(agebs, how="intersection").assign(
            area_frac=lambda df: df.area.div(df["ageb_area"]),
            pop_fraction=lambda df: df["area_frac"].mul(df["pobtot"]),
        )

        return (
            intersection.groupby("codigo")
            .agg(
                {
                    "pop_fraction": "sum",
                },
            )
            .reset_index()
            .merge(mesh, how="inner", on="codigo", validate="one_to_one")
            .pipe(lambda df: gpd.GeoDataFrame(df, crs=mesh.crs, geometry="geometry"))
        )

    return _asset


dassets = [reprojected_factory(year) for year in (1990, 2000, 2010, 2020)]
