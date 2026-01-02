"""
Dr Nneoma O
Analyse - a simple summary table for reporting
"""
import pandas as pd

def summarise_breaches(flagged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Produces a simple summary table for reporting.
    """
    summary = (
        flagged_df["SPCStatus"]
        .value_counts(dropna=False)
        .rename_axis("SPCStatus")
        .reset_index(name="Days")
    )
    return summary
