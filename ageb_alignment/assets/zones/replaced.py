import geopandas as gpd
import numpy as np

from ageb_alignment.configs.replacement import (
    replace_1990_2000,
    replace_2000_2010,
    replace_1990_2010,
)
from ageb_alignment.partitions import zone_partitions
from dagster import asset, AssetExecutionContext, AssetIn


def replace_geoms(
    gdf_old: gpd.GeoDataFrame,
    gdf_new: gpd.GeoDataFrame,
    replace_list: list,
    check_complete=False,
) -> gpd.GeoDataFrame:
    """Replaces geometries in old with the union of geometries in new following the
    corresponding relations in replace_list."""

    gdf_old = gdf_old.copy()

    # Check if replacement covers all GDF without repeated indices
    old_idx = []
    for oi, _ in replace_list:
        if isinstance(oi, str):
            old_idx.append(oi)
        else:
            old_idx += oi
    if check_complete:
        assert np.all(sorted(old_idx) == gdf_old.index)

    # Check targets are unique
    targets = [tl for _, tl in replace_list]
    targets_flat = sum(targets, [])
    assert len(np.unique(targets_flat)) == len(targets_flat)

    for old_id, new_ids in replace_list:
        if isinstance(old_id, str):
            # This is a one to one or one to many relation
            gdf_old.loc[old_id, "geometry"] = gdf_new.loc[
                new_ids, "geometry"
            ].union_all()
        elif isinstance(old_id, list):
            # This is many to one relation or many to many
            pobsum = gdf_old.loc[old_id, "POBTOT"].sum()
            gdf_old = gdf_old.drop(old_id)
            new_geom = gdf_new.loc[new_ids, "geometry"].union_all()
            gdf_old.loc["+".join(old_id)] = [pobsum, new_geom]
        else:
            raise NotImplementedError

    return gdf_old.sort_index()


@asset(
    name="2000",
    key_prefix=["zone_agebs", "replaced"],
    ins={
        "agebs_old": AssetIn(["zone_agebs", "initial", "2000"]),
        "agebs_new": AssetIn(["zone_agebs", "initial", "2010"]),
    },
    partitions_def=zone_partitions,
    io_manager_key="geojson_manager",
)
def zones_replaced_2000(
    context: AssetExecutionContext,
    agebs_old: gpd.GeoDataFrame,
    agebs_new: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    zone = context.partition_key

    agebs_old = agebs_old.set_index("CVEGEO")
    agebs_new = agebs_new.set_index("CVEGEO")

    if zone in replace_2000_2010:
        agebs_replaced = replace_geoms(agebs_old, agebs_new, replace_2000_2010[zone])
        context.log.info(f"Replaced {zone} with 2010 AGEBs.")
    else:
        agebs_replaced = agebs_old

    return agebs_replaced


@asset(
    name="1990",
    key_prefix=["zone_agebs", "replaced"],
    ins={
        "agebs_1990": AssetIn(["zone_agebs", "switched", "1990"]),
        "agebs_2000": AssetIn(["zone_agebs", "initial", "2000"]),
        "agebs_2010": AssetIn(["zone_agebs", "initial", "2010"]),
    },
    partitions_def=zone_partitions,
    io_manager_key="geojson_manager",
)
def zones_replaced_1990(
    context: AssetExecutionContext,
    agebs_1990: gpd.GeoDataFrame,
    agebs_2000: gpd.GeoDataFrame,
    agebs_2010: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    zone = context.partition_key

    agebs_1990 = agebs_1990.set_index("CVEGEO")
    agebs_2000 = agebs_2000.set_index("CVEGEO")
    agebs_2010 = agebs_2010.set_index("CVEGEO")

    if zone in replace_1990_2010:
        agebs_replaced = replace_geoms(agebs_1990, agebs_2010, replace_1990_2010[zone])
        context.log.info(f"Replaced {zone} with 2010 AGEBs.")
    elif zone in replace_1990_2000:
        agebs_replaced = replace_geoms(agebs_1990, agebs_2000, replace_1990_2000[zone])
        context.log.info(f"Replaced {zone} with 2000 AGEBs.")
    else:
        agebs_replaced = agebs_1990

    return agebs_replaced


zones_replace_assets = [zones_replaced_1990, zones_replaced_2000]
