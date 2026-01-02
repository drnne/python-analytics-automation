"""
Dr N.O 
Extract SQL: A module that extracts data from an SQL server and returns it as a dataframe using pandas for further processing
This calls on the Config file created earlier and is good as it keeps SQL logic separate from analysis. IF it fails, likely config needs checking. 

"""
import pandas as pd
import pyodbc # I import pyodbc so I can connect to SQL Server using ODBC
from .config import Config


# I define a helper function to build a SQL Server connection string using values stored in the Config object
def _build_conn_str(cfg: Config) -> str:
    # I am using SQL authentication here but this can be adapted for Windows authentication 
    return (
        f"DRIVER={{{cfg.sql_driver}}};"
        f"SERVER={cfg.sql_server};"
        f"DATABASE={cfg.sql_database};"
        f"UID={cfg.sql_username};"
        f"PWD={cfg.sql_password};"
        "TrustServerCertificate=yes;"
    )


# I define a function to extract infection event data from SQL Server
# The SQL query itself is passed in as an argument, keeping this function reusable and decoupled from specific business logic
def extract_infection_events_sql(cfg: Config, query: str) -> pd.DataFrame:
    """
    Extract data from SQL Server using pyodbc.

    If SQL connection details are not provided, the caller should
    fall back to synthetic or test data instead.
    """

    # I check that all required SQL configuration values are present
    # If any are missing, I raise a clear error early rather than failing later with a cryptic database connection error
    if not all([
        cfg.sql_server,
        cfg.sql_database,
        cfg.sql_username,
        cfg.sql_password
    ]):
        raise ValueError(
            "SQL configuration is incomplete. "
            "Provide SQL_* values in .env or use synthetic data."
        )

    conn_str = _build_conn_str(cfg)    # Build the SQL Server connection string from the config

    # I open a database connection using a context manager
    # This ensures the connection is always closed cleanly, even if an error occurs
  
    with pyodbc.connect(conn_str) as conn: 
        df = pd.read_sql(query, conn) # Execute SQL and put into a pandas DataFrame
    return df
