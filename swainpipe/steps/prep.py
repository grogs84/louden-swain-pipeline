
import pandas as pd

"""
This module provides functions that perform initial cleaning steps on a DataFrame.
It includes functions to lowercase all string values, lower case column names, and trim whitespace.
These functions are intended to be used as part of a data processing pipeline.

The raw data had inconsistent casing and whitespace issues, which these functions address.
"""


def run(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the input DataFrame by changing the case to lower.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to clean.

    Returns:
    pd.DataFrame: The cleaned DataFrame.
    """

    df = clean_columns(df)
    df = lowercase_strings(df)
    # df = convert_id_to_string(df)
    return df

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lowercase all column names and replacing space with underscores.

    Parameters:
    df (pd.DataFrame): The DataFrame to process.

    Returns:
    pd.DataFrame: The DataFrame with lowercased and space-trimmed column names.
    """
    df.columns = df.columns.map(lambda x: x.strip().lower().replace(" ", "_").strip())
    return df

def lowercase_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lowercase all string values in the DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame to process.

    Returns:
    pd.DataFrame: The DataFrame with all string values lowercased.
    """
    for col in df.select_dtypes(include="object").columns:
        df.loc[:, col] = df[col].str.lower().str.strip()
    return df

def convert_id_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all numeric IDs in the DataFrame to string type.

    Parameters:
    df (pd.DataFrame): The DataFrame to process.

    Returns:
    pd.DataFrame: The DataFrame with all numeric IDs converted to string.
    """
    df['w_wrestler_id'] = df['w_wrestler_id'].astype(str)
    df['l_wrestler_id'] = df['l_wrestler_id'].astype(str)
    return df