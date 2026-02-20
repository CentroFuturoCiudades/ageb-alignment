from pathlib import Path

import pandas as pd

import dagster as dg
from ageb_alignment.defs.resources import PathResource


def load_all_census_files_factory(year: int) -> dg.OpDefinition:
    @dg.op(name=f"load_census_op_{year}")
    def _op(path_resource: PathResource) -> pd.DataFrame:
        census_path = Path(path_resource.raw_path) / f"census/INEGI/{year}"

        df = []
        for path in census_path.glob("*.csv"):
            temp = (
                pd.read_csv(
                    path,
                    low_memory=False,
                    na_values=["*", "N/D"],
                    usecols=lambda x: x.upper()
                    in [
                        "ENTIDAD",
                        "NOM_ENT",
                        "MUN",
                        "NOM_MUN",
                        "LOC",
                        "AGEB",
                        "MZA",
                        "POBTOT",
                    ],
                )
                .pipe(lambda df: df.set_axis(df.columns.str.upper(), axis=1))
                .rename(
                    columns={
                        "ENTIDAD": "CVE_ENT",
                        "MUN": "CVE_MUN",
                        "LOC": "CVE_LOC",
                        "AGEB": "CVE_AGEB",
                        "MZA": "CVE_MZA",
                    },
                )
            )
            df.append(temp)
        return pd.concat(df)

    return _op


load_all_census_files_ops = {
    year: load_all_census_files_factory(year) for year in (2010, 2020)
}


@dg.op(out=dg.Out(io_manager_key="csv_manager"))
def get_agebs_from_census(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.query("(CVE_MZA == 0) & (CVE_AGEB != '0000') & (CVE_AGEB != '0')")
        .drop(columns=["CVE_MZA"])
        .assign(
            CVE_ENT=lambda df: df["CVE_ENT"].astype(str).str.zfill(2),
            CVE_MUN=lambda df: df["CVE_MUN"].astype(str).str.zfill(3),
            CVE_LOC=lambda df: df["CVE_LOC"].astype(str).str.zfill(4),
            CVE_AGEB=lambda df: df["CVE_AGEB"].astype(str).str.zfill(4),
            CVEGEO=lambda df: df["CVE_ENT"]
            + df["CVE_MUN"]
            + df["CVE_LOC"]
            + df["CVE_AGEB"],
        )
        .set_index("CVEGEO")
        .sort_index()
        .drop(
            columns=["CVE_ENT", "CVE_MUN", "CVE_LOC", "CVE_AGEB", "NOM_ENT", "NOM_MUN"],
        )
    )


@dg.op(out=dg.Out(io_manager_key="csv_manager"))
def get_blocks_from_census(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.query("CVE_MZA != 0")
        .assign(
            CVEGEO=lambda df: df.CVE_ENT.astype(str).str.pad(2, "left", "0")
            + df.CVE_MUN.astype(str).str.pad(3, "left", "0")
            + df.CVE_LOC.astype(str).str.pad(4, "left", "0")
            + df.CVE_AGEB.astype(str).str.pad(4, "left", "0")
            + df.CVE_MZA.astype(str).str.pad(3, "left", "0"),
        )
        .set_index("CVEGEO")
        .sort_index()
        .drop(columns=["CVE_ENT", "CVE_MUN", "CVE_LOC", "CVE_AGEB", "CVE_MZA"])
    )


@dg.op(
    out={
        "ageb": dg.Out(is_required=False, io_manager_key="csv_manager"),
        "blocks": dg.Out(is_required=False, io_manager_key="csv_manager"),
    },
)
def agebs_blocks_dispatcher(context: dg.OpExecutionContext, census: pd.DataFrame):
    if "ageb" in context.selected_output_names:
        yield dg.Output(get_agebs_from_census(census), output_name="ageb")
    if "blocks" in context.selected_output_names:
        yield dg.Output(get_blocks_from_census(census), output_name="blocks")


# pylint: disable=no-value-for-parameter
def inegi_factory(year: int) -> dg.AssetsDefinition:
    @dg.graph_multi_asset(
        name=f"census_multi_asset_{year}",
        outs={
            "agebs": dg.AssetOut(key=["census", str(year), "ageb"]),
            "blocks": dg.AssetOut(key=["census", str(year), "blocks"]),
        },
        can_subset=True,
        group_name=f"census_{year}",
    )
    def _asset():
        df = load_all_census_files_ops[year]()
        ageb, blocks = agebs_blocks_dispatcher(df)
        return {
            "agebs": ageb,
            "blocks": blocks,
        }

    return _asset


dassets = [inegi_factory(year) for year in (2010, 2020)]
