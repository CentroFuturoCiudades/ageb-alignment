from unittest.mock import MagicMock, patch

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import box

import dagster as dg
from ageb_alignment.defs.assets.mesh.merge import merge_meshes
from ageb_alignment.defs.assets.mesh.reproject import (
    load_mesh,
    reprojected_factory,
)
from ageb_alignment.defs.assets.mesh.validation import validate_cell_codes

YEARS = (1990, 2000, 2010, 2020)


def require_geodataframe(value: object) -> gpd.GeoDataFrame:
    if not isinstance(value, gpd.GeoDataFrame):
        pytest.fail(f"Expected a GeoDataFrame, got {type(value).__name__}")
    return value


def cells(codes: list[str | None], *, offset: int = 0) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "codigo": pd.Series(codes, dtype="object"),
            "pop_fraction": [float(i + 10) for i in range(len(codes))],
        },
        geometry=[box(i + offset, 0, i + offset + 1, 1) for i in range(len(codes))],
        crs="EPSG:3857",
    )


@pytest.mark.parametrize("codes", [["a", "a"], ["a", None]])
def test_validation_rejects_invalid_codes(codes: list[str | None]) -> None:
    with pytest.raises(ValueError, match="test source: codigo must be"):
        validate_cell_codes(cells(codes), source="test source")


def test_loader_rejects_duplicate_source_rows() -> None:
    resource = MagicMock()
    context = dg.build_asset_context(partition_key="09.1.01")
    with (
        patch(
            "ageb_alignment.defs.assets.mesh.reproject.gpd.read_postgis",
            return_value=cells(["a", "a"]),
        ),
        pytest.raises(ValueError, match=r"mesh partition 09\.1\.01.*duplicate.*a"),
    ):
        load_mesh(context, resource)


@pytest.mark.parametrize("year", YEARS)
def test_reprojection_sums_contributions_once(year: int) -> None:
    mesh = cells(["a", "b"]).drop(columns="pop_fraction")
    agebs = gpd.GeoDataFrame(
        {"pobtot": [100.0, 40.0]},
        geometry=[box(0, 0, 2, 0.5), box(0, 0.5, 2, 1)],
        crs=mesh.crs,
    )
    result = require_geodataframe(reprojected_factory(year)(mesh, agebs))
    np.testing.assert_equal(result.codigo.tolist(), ["a", "b"])
    np.testing.assert_allclose(result.pop_fraction.tolist(), [70.0, 70.0])
    np.testing.assert_equal(result.geometry.equals(mesh.geometry), desired=True)
    np.testing.assert_equal(result.crs, mesh.crs)


@pytest.mark.parametrize("codes", [["a", "a"], ["a", None]])
def test_reprojection_rejects_invalid_mesh_before_overlay(
    codes: list[str | None],
) -> None:
    with (
        patch.object(gpd.GeoDataFrame, "overlay") as overlay,
        pytest.raises(ValueError, match="mesh input for reprojection 2020"),
    ):
        reprojected_factory(2020)(cells(codes), cells([]))
    overlay.assert_not_called()


def test_merge_preserves_union_values_and_earliest_geometry() -> None:
    inputs = [
        cells(["shared", str(year)], offset=i * 10) for i, year in enumerate(YEARS)
    ]
    result = require_geodataframe(merge_meshes(*inputs)).set_index("codigo")
    np.testing.assert_equal(result.index.is_unique, desired=True)
    np.testing.assert_equal(
        set(result.index), {"shared", *(str(year) for year in YEARS)}
    )
    for i, year in enumerate(YEARS):
        column = f"pop_fraction_{year}"
        np.testing.assert_equal(result.loc["shared", column], 10.0)
        np.testing.assert_equal(result.loc[str(year), column], 11.0)
        np.testing.assert_equal(
            result.loc[str(year), "geometry"], inputs[i].geometry.iloc[1]
        )
        for other in YEARS:
            if other != year:
                np.testing.assert_equal(
                    pd.isna(result.loc[str(other), column]), desired=True
                )
    np.testing.assert_equal(
        result.loc["shared", "geometry"], inputs[0].geometry.iloc[0]
    )
    np.testing.assert_equal(
        set(result.columns), {"geometry", *(f"pop_fraction_{y}" for y in YEARS)}
    )


@pytest.mark.parametrize("year", YEARS)
@pytest.mark.parametrize("codes", [["a", "a"], ["a", None]])
def test_merge_rejects_invalid_year(year: int, codes: list[str | None]) -> None:
    inputs = [cells(codes if y == year else ["a"]) for y in YEARS]
    with pytest.raises(ValueError, match=f"reprojected/base/{year}"):
        merge_meshes(*inputs)


@pytest.mark.parametrize("populated_year", [None, *YEARS])
def test_merge_handles_empty_years(populated_year: int | None) -> None:
    inputs = [cells(["a"] if y == populated_year else []) for y in YEARS]
    result = require_geodataframe(merge_meshes(*inputs))
    np.testing.assert_equal(len(result), (0 if populated_year is None else 1))
    if populated_year is not None:
        np.testing.assert_equal(result.codigo.tolist(), ["a"])
        np.testing.assert_equal(result.geometry.iloc[0], box(0, 0, 1, 1))
        np.testing.assert_equal(result[f"pop_fraction_{populated_year}"].iloc[0], 10.0)


def test_validation_rejects_identical_copies() -> None:
    frame = cells(["a"])
    repeated = pd.concat([frame, frame], ignore_index=True)
    with pytest.raises(ValueError, match=r"duplicate codes include.*a"):
        validate_cell_codes(repeated, source="mesh")
