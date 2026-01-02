"""
Dr Nneoma O
Configuration - single source of truth! (Under source code - src)
Purpose: The purpose of this code is to create a one-stop shop where all configurations for the pipeline reside.
This code allows me to manage sensitive credentials and access without hard-coding. 

"""

from dataclasses import dataclass
import os
from dotenv import load_dotenv # I use python-dotenv to load environment variables from a .env file
load_dotenv()

# I define a Config class to hold all configuration values for the project (database connections, API credentials, and folder paths).
@dataclass(frozen=True) # I set frozen=True to make the configuration immutable, so values cannot be changed accidentally once the application is running.
class Config:
    # I read the SQL Server connection details from environment variables and if a variable is missing, I default to an empty string.
    sql_server: str = os.getenv("SQL_SERVER", "")
    sql_database: str = os.getenv("SQL_DATABASE", "")
    sql_username: str = os.getenv("SQL_USERNAME", "")
    sql_password: str = os.getenv("SQL_PASSWORD", "")
    sql_driver: str = os.getenv(
        "SQL_DRIVER",
        "ODBC Driver 17 for SQL Server"
    )

    # Similarly,  I store API connection details so they are managed in one place
    api_base_url: str = os.getenv("API_BASE_URL", "")
    api_token: str = os.getenv("API_TOKEN", "")

  """
    Project directory structure
    I define standard paths for raw data, processed data, reports, and charts. Keeping these in the config makes the pipeline easier to maintain and avoids hard-coded paths throughout the project.
  """
    raw_dir: str = "data/raw"
    processed_dir: str = "data/processed"
    reports_dir: str = "outputs/reports"
    charts_dir: str = "outputs/charts"
