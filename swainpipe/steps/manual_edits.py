import pandas as pd


"""
manual edits

rows with w_name == 'steve' and l_name == 'st john'
    or rows with w_name == 'steve' and l_name == 'st john'
need to have 'st john' replaced with 'st. john'

tim needs his wrestler id copied
joe needs to be joey
steve st. john has a misspelling
christopher is chris in the rest of his matches
"""

def run(df: pd.DataFrame) -> pd.DataFrame:
    """

    """
    df = st_john(df)
    df = tim(df)
    df = joe(df)
    df = chris(df)
    df = wrestlers_w_same_id(df)
    return df


def st_john(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace 'st john' with 'st. john' in the l_name column.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to process.

    Returns:
    pd.DataFrame: The DataFrame with 'st john' replaced by 'st. john'.
    """
    df.loc[df.match_id.isin([19904, 20458]), 'l_last'] = 'st. john'
    df.loc[df.match_id.isin([19904, 20458]), 'l_wrestler_id'] = 101375.0
    return df

def tim(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find Tim's wrestler ID and copy it to the row where Tim has a null wrestler ID.
    """

    df.loc[df.match_id == 10470, 'l_wrestler_id'] = 104486.0
    return df

def joe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace 'joe' with 'joey' for joey
    """

    df.loc[df.match_id == 19422, 'l_first'] = 'joey'
    df.loc[df.match_id == 19422, 'l_wrestler_id'] = 56629.0
    return df

def chris(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace 'christopher' with 'chris' for chris
    """
    df.loc[df.match_id == 29277, 'l_first'] = 'chris'
    df.loc[df.match_id == 29277, 'l_wrestler_id'] = 22331.0

    return df


def wrestlers_w_same_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find all wrestlers with the same wrestler ID and return their rows. 
    """
    # james|blair	
    df.loc[df.match_id == 33, 'l_wrestler_id'] = 1.5
    df.iloc[df.match_id == 33, 'w_wrestler_id'] = 49064.0

    # frank|jordan
    df.loc[df.match_id == 3928, 'l_wrestler_id'] = 39622.0

    df.loc[df.match_id == 7325, 'l_wrestler_id'] = 12924.0

    df.loc[df.match_id == 10470, 'w_wrestler_id'] = 140911.0

    df.loc[df.match_id == 18806, 'l_wrestler_id'] = 97172.0

    return df
