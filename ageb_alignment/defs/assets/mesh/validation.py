import pandas as pd


def validate_cell_codes(cells: pd.DataFrame, *, source: str) -> None:
    codes = cells["codigo"]
    if codes.isna().any():
        msg = (
            f"{source}: codigo must be non-null ({int(codes.isna().sum())} null rows)."
        )
        raise ValueError(msg)
    repeated = codes[codes.duplicated(keep=False)]
    if not repeated.empty:
        examples = repeated.drop_duplicates().head(5).tolist()
        msg = f"{source}: codigo must be unique; duplicate codes include {examples!r}."
        raise ValueError(msg)
