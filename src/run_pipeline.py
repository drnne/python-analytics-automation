"""
Dr Nneoma O
The pipeline: Bring it all together!

"""
import os
import pandas as pd
import matplotlib.pyplot as plt

from .config import Config
from .logging_utils import get_logger
from .extract_sql import extract_infection_events_sql
from .transform import standardise_infection_events
from .validate import validate_infection_events
from .spc import (
    daily_counts,
    add_zero_days,
    split_baseline_and_current,
    spc_limits_from_baseline,
    flag_breaches_against_limits,
)
from .analyse import summarise_breaches

def _ensure_dirs(cfg: Config) -> None:
    os.makedirs(cfg.raw_dir, exist_ok=True)
    os.makedirs(cfg.processed_dir, exist_ok=True)
    os.makedirs(cfg.reports_dir, exist_ok=True)
    os.makedirs(cfg.charts_dir, exist_ok=True)

def _synthetic_infection_events() -> pd.DataFrame:
    # Synthetic example mimicking a realistic event table
    dates = pd.date_range("2022-01-01", "2022-03-31", freq="D")
    # baseline low counts + a few spikes
    base = pd.Series([0, 1, 2, 1, 0, 1, 2], index=range(7))
    daily = [int(base[i % 7]) for i in range(len(dates))]
    # inject spikes
    for spike_day in ["2022-02-10", "2022-03-05"]:
        idx = (dates == spike_day).argmax()
        daily[idx] = 8

    rows = []
    event_id = 1
    for dt, n in zip(dates, daily):
        for _ in range(n):
            rows.append({"EventID": event_id, "CollectionDate": dt.date()})
            event_id += 1

    return pd.DataFrame(rows)

def main():
    cfg = Config()
    logger = get_logger("pipeline")
    _ensure_dirs(cfg)

    logger.info("Starting pipeline run.")

    # 1) Extract
    sql_query = """
    SELECT EventID, CollectionDate
    FROM dbo.InfectionEvents
    WHERE CollectionDate IS NOT NULL;
    """

    try:
        df_raw = extract_infection_events_sql(cfg, sql_query)
        logger.info("Extracted data from SQL Server.")
    except Exception as ex:
        logger.info(f"SQL extraction not used ({ex}). Falling back to synthetic dataset.")
        df_raw = _synthetic_infection_events()

    raw_path = os.path.join(cfg.raw_dir, "infection_events_raw.csv")
    df_raw.to_csv(raw_path, index=False)
    logger.info(f"Raw data saved: {raw_path}")

    # 2) Transform
    df_std = standardise_infection_events(df_raw)

    # 3) Validate
    checks = validate_infection_events(df_std)
    logger.info(f"Validation summary: {checks}")

    processed_path = os.path.join(cfg.processed_dir, "infection_events_processed.csv")
    df_std.to_csv(processed_path, index=False)
    logger.info(f"Processed data saved: {processed_path}")

             
   # 4) SPC (Baseline = previous financial year; monitor = current financial year)
             
    daily = daily_counts(df_std)
    daily_full = add_zero_days(daily)  # includes zero-case days for the full date range
    baseline, current, current_fy = split_baseline_and_current(daily_full, current_fy=None)
    
    # If you want to ensure baseline/current include all days within each FY, you can re-zero-fill within those FY windows as well:
    if not baseline.empty:
        baseline = add_zero_days(baseline[["CollectionDate", "DailyCases"]].copy(),
                                 start_date=baseline["CollectionDate"].min(),
                                 end_date=baseline["CollectionDate"].max())
    if not current.empty:
        current = add_zero_days(current[["CollectionDate", "DailyCases"]].copy(),
                                start_date=current["CollectionDate"].min(),
                                end_date=current["CollectionDate"].max())
    
    limits = spc_limits_from_baseline(baseline)
    
    flagged_current = flag_breaches_against_limits(current, limits)
    flagged_path = os.path.join(cfg.processed_dir, f"daily_spc_flagged_FY{current_fy}.csv")
    flagged_current.to_csv(flagged_path, index=False)
    logger.info(f"SPC flagged output saved: {flagged_path}")
    logger.info(f"Baseline FY: FY{current_fy-1} | Current FY: FY{current_fy}")
    logger.info(f"SPC limits from baseline FY{current_fy-1}: {limits}")
    

    # 5) Summary report
    summary = summarise_breaches(flagged)
    report_path = os.path.join(cfg.reports_dir, "spc_breach_summary.csv")
    summary.to_csv(report_path, index=False)
    logger.info(f"Summary report saved: {report_path}")

    # 6) Simple chart output (matplotlib)
    plt.figure()
    plt.plot(flagged_current["CollectionDate"], flagged_current["DailyCases"])
    plt.axhline(limits["uwl"], linestyle="--")
    plt.axhline(limits["ucl"], linestyle="--")
    plt.title(f"Daily Cases (FY{current_fy}) vs Baseline SPC Limits (FY{current_fy-1})")
    plt.xlabel("Date")
    plt.ylabel("Daily cases")
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    chart_path = os.path.join(cfg.charts_dir, f"daily_cases_spc_FY{current_fy}.png")
    plt.savefig(chart_path, dpi=150)
    logger.info(f"Chart saved: {chart_path}")


    logger.info("Pipeline run completed successfully.")

if __name__ == "__main__":
    main()
