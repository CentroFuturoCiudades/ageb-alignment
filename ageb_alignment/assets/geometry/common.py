import geopandas as gpd
import numpy as np


def fix_overlapped(gdf: gpd.GeoDataFrame, cover_list: list) -> gpd.GeoDataFrame:
    """Fix geometries covering whole other geometries by removing the overlapping part."""
    gdf = gdf.copy()
    cover_list = np.array(cover_list)

    for i in range(len(cover_list)):
        new_geoms = (
            gdf.loc[cover_list[i : i + 1, 0]]
            .geometry.difference(
                gdf.loc[cover_list[i : i + 1, 1]].geometry, align=False
            )
            .explode()
            .to_frame()
            .assign(AREA=lambda df: df.area)
            .sort_values("AREA", ascending=False)
            .groupby("CVEGEO")
            .first()
            .rename(columns={0: "geometry"})
            .set_geometry("geometry")
        )

        gdf.loc[cover_list[i : i + 1, 0], "geometry"] = new_geoms.loc[
            cover_list[i : i + 1, 0], "geometry"
        ]

    return gdf


def replace_geoms(
    gdf_old: gpd.GeoDataFrame,
    gdf_new: gpd.GeoDataFrame,
    replace_list: list,
    check_complete=False,
) -> gpd.GeoDataFrame:
    """Replaces geometries in old with the union of geometries in new following the
    corresponding relations in replace_list."""

    gdf_old = gdf_old.copy()

    old_idx = [oi for oi, _ in replace_list]
    targets = [tl for _, tl in replace_list]

    if check_complete:
        assert np.all(sorted(old_idx) == gdf_old.index)

    # Check targets are unique
    targets_flat = sum(targets, [])
    assert len(np.unique(targets_flat)) == len(targets_flat)

    for old_id, new_ids in replace_list:
        gdf_old.loc[old_id, "geometry"] = gdf_new.loc[new_ids, "geometry"].union_all()

    return gdf_old
