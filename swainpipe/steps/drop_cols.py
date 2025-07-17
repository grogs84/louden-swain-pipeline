import logging
import json
import pandas as pd


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
The raw data is coming in a an excel file that sometimes has extra columns.
"""


def run(df: pd.DataFrame) -> pd.DataFrame:    
    """
    Drops unnecessary columns from the DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame to process.

    Returns:
    pd.DataFrame: The DataFrame with only the specified columns retained.
    """
    df = drop_columns(df)
    return df


def drop_columns(df: pd.DataFrame) -> pd.DataFrame:
    """    
    Drops columns from the DataFrame that are not in the predefined list of columns to keep.

    Parameters:
    df (pd.DataFrame): The DataFrame from which columns will be dropped.

    Returns:
    pd.DataFrame: The DataFrame with only the specified columns retained.
    """
    df['weight'] = df['wt']
    cols_to_keep = ['sex', 'div', 'match_id', 'year', 'w_wrestler_id', 'l_wrestler_id',
                    'w_first', 'w_last', 'w_school', 'l_first', 'l_last','l_school',
                    'weight', 'round', 'type', 'w_score', 'l_score', 'min', 'sec','ot',
                    'w_seed', 'l_seed', 'type_notes', 'notes', 'w_name_key','l_name_key',
                    'w_wrestler_id_filled', 'l_wrestler_id_filled', 'w_name','l_name']
    cols_to_drop = [col for col in df.columns if col not in cols_to_keep]
    if not cols_to_drop:
        logger.info("No columns to drop, all required columns are present.")
        return df
    df = df.drop(columns=cols_to_drop, errors='ignore')
    logger.info(f"Dropped columns: {cols_to_drop}")
    return df