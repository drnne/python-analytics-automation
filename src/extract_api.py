"""
Dr Nneoma O 
Extract API: An easy, reusable method to extract from REST APIs and convert the response to a df 
1. Build the API URL safely
2. Handles authentication (if needed)
3. Validates response format and fails if there is a problem
5. Returns data in standard format for the pipeline

"""

import pandas as pd
import requests # I import requests so I can make HTTP calls to external APIs
from .config import Config # Again importing from Config


# I define a generic function to extract data from an API endpoint.
# The endpoint is passed in so this function can be reused for different API resources.

def extract_from_api(cfg: Config, endpoint: str) -> pd.DataFrame:
    """
    Generic API extractor.

    Expects a JSON response in one of the following formats:
    - A list of dictionaries
    - A dictionary containing a 'data' key with a list of dictionaries
    """

    # I check that the API base URL has been provided in the configuration
    # If it is missing, I fail early with a clear error message
    if not cfg.api_base_url:
        raise ValueError("API_BASE_URL not set in .env")

    # I construct the full API URL safely by removing any trailing or leading slashes to avoid malformed URLs.
    url = cfg.api_base_url.rstrip("/") + "/" + endpoint.lstrip("/")

    # I prepare the request headers (these are empty by default and only populated if an API token exists)
    headers = {}

    # If an API token is provided, I add it as a Bearer token in the authorisation header
    if cfg.api_token:
        headers["Authorization"] = f"Bearer {cfg.api_token}"

    # I send a GET request to the API with a 30-second timeout to prevent the pipeline from hanging indefinitely
    resp = requests.get(url, headers=headers, timeout=30)

    # I raise an exception automatically if the response status code indicates an error (e.g. 4xx or 5xx)
    resp.raise_for_status()

    # I parse the JSON response body into a Python object
    payload = resp.json()

    # Some APIs wrap the actual data inside a 'data' key - if this is the case, I extract the list stored there.
    if isinstance(payload, dict) and "data" in payload:
        payload = payload["data"]

    # I validate that the final payload is a list of records.
    # If it is not, I raise an explicit error explaining that the API response format is not supported.
    if not isinstance(payload, list):
        raise ValueError(
            "API response format not recognised. "
            "Expected list or dict with 'data' list."
        )

    # I convert the list of dictionaries into a pandas DataFrame and return it for transformation, validation, and analysis
    return pd.DataFrame(payload)
