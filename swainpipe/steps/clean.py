
import pandas as pd


def run(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the input DataFrame by removing rows with missing values.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to clean.

    Returns:
    pd.DataFrame: The cleaned DataFrame.
    """

    df.dropna(inplace=True)
    return df