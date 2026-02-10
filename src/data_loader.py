"""
Data loader for Chicago Crimes (2001–Present) dataset.

This module is responsible ONLY for downloading raw data from the
City of Chicago Open Data Portal using the Socrata Open Data API (SODA).
No data cleaning or feature engineering is performed here.
"""

import os
import requests
import pandas as pd
from typing import Optional, List, Dict


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

BASE_API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
DEFAULT_BATCH_SIZE = 50000  # Max recommended batch size for SODA


# ---------------------------------------------------------------------
# Core API functions
# ---------------------------------------------------------------------

def fetch_chicago_data(
    limit: int,
    offset: int,
    where: Optional[str] = None,
    select: Optional[str] = None
) -> List[Dict]:
    """
    Fetch a single batch of raw data from the Chicago Open Data API.

    Args:
        limit: Number of records to fetch
        offset: Offset for pagination
        where: SQL-like WHERE clause (optional)
        select: Columns to select (optional)

    Returns:
        List of raw records (each record is a dictionary)
    """
    params = {
        "$limit": limit,
        "$offset": offset
    }

    if where:
        params["$where"] = where
    if select:
        params["$select"] = select

    response = requests.get(BASE_API_URL, params=params)
    response.raise_for_status()

    return response.json()


def fetch_all_chicago_data(
    where: Optional[str] = None,
    batch_size: int = DEFAULT_BATCH_SIZE
) -> pd.DataFrame:
    """
    Fetch all records that satisfy the WHERE clause using pagination.

    Args:
        where: SQL-like WHERE clause
        batch_size: Number of records per API request

    Returns:
        pandas DataFrame containing ALL raw records
    """
    all_records: List[Dict] = []
    offset = 0

    while True:
        print(f"Fetching records {offset} to {offset + batch_size} ...")

        batch = fetch_chicago_data(
            limit=batch_size,
            offset=offset,
            where=where
        )

        if not batch:
            break

        all_records.extend(batch)
        offset += batch_size

    return pd.DataFrame(all_records)


# ---------------------------------------------------------------------
# IO utilities
# ---------------------------------------------------------------------

def save_to_csv(df: pd.DataFrame, filepath: str) -> None:
    """
    Save DataFrame to CSV.

    Args:
        df: DataFrame to save
        filepath: Output file path
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    df.to_csv(filepath, index=False)
    print(f"Saved raw data to {filepath}")


# ---------------------------------------------------------------------
# Script entry point
# ---------------------------------------------------------------------

if __name__ == "__main__":
    print("Starting raw Chicago crime data download...")

    # -------------------------
    # Training data: 2015–2024
    # -------------------------
    train_where = "year >= 2015 AND year <= 2024"
    df_train = fetch_all_chicago_data(where=train_where)

    print(f"Training set size: {len(df_train):,}")
    print(f"Training columns: {list(df_train.columns)}")

    save_to_csv(
        df_train,
        "data/raw/chicago_crimes_2015_2024_raw.csv"
    )

    # -------------------------
    # Test data: 2025
    # -------------------------
    test_where = "year = 2025"
    df_test = fetch_all_chicago_data(where=test_where)

    print(f"Test set size: {len(df_test):,}")

    save_to_csv(
        df_test,
        "data/raw/chicago_crimes_2025_raw.csv"
    )

    print("Raw data download completed successfully.")
