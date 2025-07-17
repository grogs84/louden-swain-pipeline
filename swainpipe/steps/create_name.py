import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
The raw data has wrestlers first and last names in separate columns.
This module provides a function to create a name/key (fname|lastname) column
by concatenating the first and last names with a pipe character.
This is useful for creating a unique identifier for each wrestler.

We need to do this for both w_first, w_last and l_first, l_last columns.

"""

def run(df: pd.DataFrame) -> pd.DataFrame:
    df['w_name'] = df['w_first'].str.strip() + "|" + df['w_last'].str.strip()
    df['l_name'] = df['l_first'].str.strip() + "|" + df['l_last'].str.strip()
    return df