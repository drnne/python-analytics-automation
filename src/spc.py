"""
SPC values - needed for analysis to compare current to previous year values
"""

import pandas as pd
import numpy as np

UK_FY_START_MONTH = 4  # April

def daily_counts(df: pd.DataFrame) -> pd.DataFrame:
    if "CollectionDate" not in df.columns:
        raise ValueError("CollectionDate is required for daily counts.")

    out = (
        df.groupby("CollectionDate", dropna=False)
          .size()
          .rename("DailyCases")
          .reset_index()
    )

    out["CollectionDate"] = pd.to_datetime(out["CollectionDate"], errors="coerce")
    return out.sort_values("CollectionDate")


def add_zero_days(daily_df: pd.DataFrame, start_date=None, end_date=None) -> pd.DataFrame:
    d = daily_df.copy()
    d["CollectionDate"] = pd.to_datetime(d["CollectionDate"], errors="coerce")

    if start_date is None:
        start_date = d["CollectionDate"].min()
    else:
        start_date = pd.to_datetime(start_date)

    if end_date is None:
        end_date = d["CollectionDate"].max()
    else:
        end_date = pd.to_datetime(end_date)

    idx = pd.date_range(start=start_date, end=end_date, freq="D")
    d = (
        d.set_index("CollectionDate")
         .reindex(idx)
         .fillna({"DailyCases": 0})
         .rename_axis("CollectionDate")
         .reset_index()
    )
    d["DailyCases"] = d["DailyCases"].astype(int)
    return d


def uk_financial_year_for_date(dt: pd.Timestamp) -> int:
    """
    Returns the financial year label as the year in which the FY ends.
    Example:
      - 2024-04-01 to 2025-03-31 => FY 2025
      - 2025-04-01 to 2026-03-31 => FY 2026
    """
    if pd.isna(dt):
        raise ValueError("Date is NaT; cannot determine financial year.")
    return dt.year + 1 if dt.month >= UK_FY_START_MONTH else dt.year


def add_financial_year(daily_df: pd.DataFrame) -> pd.DataFrame:
    d = daily_df.copy()
    d["CollectionDate"] = pd.to_datetime(d["CollectionDate"], errors="coerce")
    d["FinancialYear"] = d["CollectionDate"].apply(uk_financial_year_for_date)
    return d


def split_baseline_and_current(daily_df: pd.DataFrame, current_fy: int | None = None) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """
    Splits daily data into:
      - baseline FY (previous FY)
      - current FY

    If current_fy is None, it uses the FY of the latest date present.
    """
    d = add_financial_year(daily_df)

    if current_fy is None:
        latest_date = d["CollectionDate"].max()
        current_fy = uk_financial_year_for_date(pd.to_datetime(latest_date))

    baseline_fy = current_fy - 1

    baseline = d[d["FinancialYear"] == baseline_fy].copy()
    current = d[d["FinancialYear"] == current_fy].copy()

    return baseline, current, current_fy


def spc_limits_from_baseline(baseline_daily_df: pd.DataFrame) -> dict:
    """
    Calculates SPC limits from baseline daily cases.
    Uses sample standard deviation (ddof=1), aligned with common SPC practice.
    """
    if baseline_daily_df.empty:
        raise ValueError("Baseline dataframe is empty. Cannot calculate SPC limits.")

    x = baseline_daily_df["DailyCases"].astype(float).to_numpy()

    mean = float(np.mean(x))
    std = float(np.std(x, ddof=1)) if len(x) > 1 else 0.0

    return {
        "mean": mean,
        "std": std,
        "uwl": mean + 2 * std,  # Upper Warning Limit (2 SD)
        "ucl": mean + 3 * std,  # Upper Control Limit (3 SD)
    }


def flag_breaches_against_limits(daily_df: pd.DataFrame, limits: dict) -> pd.DataFrame:
    """
    Applies baseline limits to a (typically current FY) daily series.
    """
    d = daily_df.copy()
    d["Mean"] = limits["mean"]
    d["UWL_2SD"] = limits["uwl"]
    d["UCL_3SD"] = limits["ucl"]

    d["SPCStatus"] = np.select(
        [
            d["DailyCases"] >= d["UCL_3SD"],
            d["DailyCases"] >= d["UWL_2SD"],
        ],
        [
            "3 SD Breach",
            "2 SD Warning",
        ],
        default="Within Expected Range",
    )
    return d
