"""
Dr Nneoma O
Validation: This code checks the quality of the event data and returns a summary of validation metrics - not modifying the data itself
1. How many records do we have>
2. Are any dates missing?
3. Are there dupliciate event IDs?
4. What date range is covered?
"""

import pandas as pd

# I define a function to perform basic data quality checks on infection event data and return the results as a dictionary
def validate_infection_events(df: pd.DataFrame) -> dict:
    """
    Returns validation results as a dictionary for reporting and logging
    """

    # I initialise an empty dictionary to store validation outputs
    # Using a dict makes it easy to log, report, or persist the results
    results = {}

    # I record the total number of rows in the dataset
    # This is a fundamental check and useful for audit trails
    results["row_count"] = int(len(df))

    # I count how many records have a missing CollectionDate
    # If the column does not exist, I return None rather than raising an error
    results["null_collectiondate"] = (
        int(df["CollectionDate"].isna().sum())
        if "CollectionDate" in df.columns
        else None
    )

    # --- Duplicate checks ---
    # If an EventID column exists, I check for duplicated IDs
    # This helps identify potential double-counting issues
    if "EventID" in df.columns:
        results["duplicate_eventid"] = int(
            df["EventID"].duplicated().sum()
        )
    else:
        results["duplicate_eventid"] = None

    # --- Date range checks ---
    # If a CollectionDate column exists, I calculate the minimum
    # and maximum dates present in the data
    if "CollectionDate" in df.columns:

        # I coerce the column to datetime to ensure invalid values
        # do not cause the calculation to fail
        non_null = pd.to_datetime(
            df["CollectionDate"],
            errors="coerce"
        )

        # I record the earliest date in the dataset, if any valid dates exist
        results["min_date"] = (
            str(non_null.min().date())
            if non_null.notna().any()
            else None
        )

        # I record the latest date in the dataset, if any valid dates exist
        results["max_date"] = (
            str(non_null.max().date())
            if non_null.notna().any()
            else None
        )

    # Finally, I return the validation results dictionary
    # so it can be logged or included in a data quality report
    return results
