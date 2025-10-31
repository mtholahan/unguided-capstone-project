import pandas as pd
import logging


def infer_schema(df: pd.DataFrame):
    """Infer a lightweight schema summary for debugging."""
    return {col: str(dtype) for col, dtype in df.dtypes.items()}


def key_health(df: pd.DataFrame, key_cols: list[str]):
    """Assess primary key uniqueness and nulls."""
    total = len(df)
    nulls = df[key_cols].isnull().sum().sum()
    dupes = df.duplicated(subset=key_cols).sum()
    logging.info(f"Key health: total={total}, nulls={nulls}, duplicates={dupes}")
    return {"total": total, "nulls": int(nulls), "duplicates": int(dupes)}
