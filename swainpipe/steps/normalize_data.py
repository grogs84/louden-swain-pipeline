import logging
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
This module normalizes the cleaned wrestling match data into separate tables:
- wrestlers: unique wrestler information with computed full names
- matches: match details without wrestler-specific information
- match_participants: linking table between wrestlers and matches with results

This creates a normalized database structure that eliminates data duplication
and maintains referential integrity.
"""


def run(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the input DataFrame into separate tables and save as CSV files.
    
    Parameters:
    df (pd.DataFrame): The cleaned DataFrame from previous pipeline steps.
    
    Returns:
    pd.DataFrame: The original DataFrame (unchanged for pipeline continuity).
    """
    logger.info("Starting data normalization process")
    
    # Create the three normalized tables
    wrestlers_df = create_wrestlers_table(df)
    matches_df = create_matches_table(df)
    match_participants_df = create_match_participants_table(df)
    
    # Save tables to CSV files in the output directory
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    wrestlers_df.to_csv(output_dir / "wrestlers.csv", index=False)
    matches_df.to_csv(output_dir / "matches.csv", index=False)
    match_participants_df.to_csv(output_dir / "match_participants.csv", index=False)
    
    logger.info(f"Created wrestlers table with {len(wrestlers_df)} unique wrestlers")
    logger.info(f"Created matches table with {len(matches_df)} matches")
    logger.info(f"Created match_participants table with {len(match_participants_df)} participant records")
    logger.info("Normalization complete - CSV files saved to output directory")
    
    return df


def create_wrestlers_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract unique wrestlers from both winner and loser data.
    
    Parameters:
    df (pd.DataFrame): The input DataFrame containing match data.
    
    Returns:
    pd.DataFrame: Normalized wrestlers table with columns:
                  wrestler_id, first_name, last_name, school, full_name
    """
    # Extract winner data
    winners = df[['w_wrestler_id', 'w_first', 'w_last', 'w_school']].copy()
    winners.columns = ['wrestler_id', 'first_name', 'last_name', 'school']
    
    # Extract loser data  
    losers = df[['l_wrestler_id', 'l_first', 'l_last', 'l_school']].copy()
    losers.columns = ['wrestler_id', 'first_name', 'last_name', 'school']
    
    # Combine and deduplicate
    all_wrestlers = pd.concat([winners, losers], ignore_index=True)
    
    # Remove duplicates based on wrestler_id, keeping first occurrence
    wrestlers = all_wrestlers.drop_duplicates(subset=['wrestler_id'], keep='first')
    
    # Handle missing wrestler_ids by dropping them
    wrestlers = wrestlers.dropna(subset=['wrestler_id'])
    
    # Convert wrestler_id to integer
    wrestlers['wrestler_id'] = wrestlers['wrestler_id'].astype(int)
    
    # Create full_name as "first_name last_name"
    wrestlers['full_name'] = wrestlers['first_name'].str.strip() + " " + wrestlers['last_name'].str.strip()
    
    # Sort by wrestler_id for consistency
    wrestlers = wrestlers.sort_values('wrestler_id').reset_index(drop=True)
    
    logger.info(f"Extracted {len(wrestlers)} unique wrestlers")
    
    return wrestlers[['wrestler_id', 'first_name', 'last_name', 'school', 'full_name']]


def create_matches_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform match data to the normalized matches table format.
    
    Parameters:
    df (pd.DataFrame): The input DataFrame containing match data.
    
    Returns:
    pd.DataFrame: Normalized matches table with match details only.
    """
    matches = df[['match_id', 'year', 'div', 'weight', 'round', 'type', 
                  'w_score', 'l_score', 'min', 'sec', 'ot', 'w_seed', 'l_seed',
                  'type_notes', 'notes']].copy()
    
    # Rename columns to match target schema
    matches = matches.rename(columns={
        'div': 'division',
        'weight': 'weight_class',
        'type': 'match_type',
        'w_score': 'winner_score',
        'l_score': 'loser_score',
        'min': 'duration_minutes',
        'sec': 'duration_seconds',
        'ot': 'overtime',
        'w_seed': 'winner_seed',
        'l_seed': 'loser_seed'
    })
    
    # Convert data types
    matches['match_id'] = matches['match_id'].astype(int)
    matches['year'] = pd.to_numeric(matches['year'], errors='coerce')
    matches['winner_score'] = pd.to_numeric(matches['winner_score'], errors='coerce')
    matches['loser_score'] = pd.to_numeric(matches['loser_score'], errors='coerce')
    matches['duration_minutes'] = pd.to_numeric(matches['duration_minutes'], errors='coerce')
    matches['duration_seconds'] = pd.to_numeric(matches['duration_seconds'], errors='coerce')
    matches['winner_seed'] = pd.to_numeric(matches['winner_seed'], errors='coerce')
    matches['loser_seed'] = pd.to_numeric(matches['loser_seed'], errors='coerce')
    
    # Convert overtime to boolean (assuming non-null/non-empty values mean overtime)
    matches['overtime'] = matches['overtime'].notna() & (matches['overtime'] != '')
    
    # Sort by match_id for consistency
    matches = matches.sort_values('match_id').reset_index(drop=True)
    
    logger.info(f"Created matches table with {len(matches)} matches")
    
    return matches


def create_match_participants_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create match_participants table linking wrestlers to matches with their results.
    
    Parameters:
    df (pd.DataFrame): The input DataFrame containing match data.
    
    Returns:
    pd.DataFrame: match_participants table with columns:
                  id, match_id, wrestler_id, result
    """
    participants = []
    
    # Add winner records
    winners = df[['match_id', 'w_wrestler_id']].copy()
    winners = winners.dropna(subset=['w_wrestler_id'])
    winners['wrestler_id'] = winners['w_wrestler_id'].astype(int)
    winners['result'] = 'win'
    winners = winners[['match_id', 'wrestler_id', 'result']]
    participants.append(winners)
    
    # Add loser records
    losers = df[['match_id', 'l_wrestler_id']].copy()
    losers = losers.dropna(subset=['l_wrestler_id'])
    losers['wrestler_id'] = losers['l_wrestler_id'].astype(int)
    losers['result'] = 'loss'
    losers = losers[['match_id', 'wrestler_id', 'result']]
    participants.append(losers)
    
    # Combine all participants
    match_participants = pd.concat(participants, ignore_index=True)
    
    # Convert match_id to integer
    match_participants['match_id'] = match_participants['match_id'].astype(int)
    
    # Add auto-incrementing id column
    match_participants['id'] = range(1, len(match_participants) + 1)
    
    # Reorder columns to match schema
    match_participants = match_participants[['id', 'match_id', 'wrestler_id', 'result']]
    
    # Sort by match_id, then by result (win first) for consistency
    match_participants = match_participants.sort_values(['match_id', 'result']).reset_index(drop=True)
    
    logger.info(f"Created match_participants table with {len(match_participants)} participant records")
    
    return match_participants