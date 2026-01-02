"""
Dr Nneoma O
Transforming data: this code essentially standardises the event data so that regardless of the source (SQL, API, CVS etc),
the rest of the pipeline can rely on consistent column names for dates, data types and a uniform set of columns. 
Aka the schema harmonisation step!
Run this once for every raw df i.e. df_sql_raw -- use this query --> df_sql_std, df_api_raw ---> df_api_std
"""
import pandas as pd

def standardise_infection_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardise column names and types to a predictable schema
    Expected minimum columns:
      - CollectionDate (date/datetime)
      - EventID (optional)
    """
    df = df.copy()

    # Common normalisation: strip spaces and unify column names - only date mentioned but update for others if needed
    df.columns = [c.strip() for c in df.columns]

    if "CollectionDate" not in df.columns:
        # Try common alternatives
        for alt in ["collection_date", "Collection_Date", "Date", "CollectionInstant"]: # Update based on the different names
            if alt in df.columns:
                df.rename(columns={alt: "CollectionDate"}, inplace=True)
                break

    if "CollectionDate" not in df.columns:
        raise ValueError("No CollectionDate column found after standardisation.")

    df["CollectionDate"] = pd.to_datetime(df["CollectionDate"], errors="coerce").dt.date

    # Keep only relevant columns (extend as needed)
    keep = [c for c in ["EventID", "CollectionDate", "Department", "Location", "MetricValue"] if c in df.columns]
    if not keep:
        keep = ["CollectionDate"]

    df = df[keep]

    return df
