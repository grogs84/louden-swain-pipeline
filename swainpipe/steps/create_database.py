import logging
import uuid
import pandas as pd
import duckdb
from pathlib import Path
from typing import Dict, Any

"""
This step creates a DuckDB database from the CSV outputs, converting integer IDs to UUIDs
and populating tables according to the schema.sql structure.

The step will:
1. Read the CSV files with match and participant data
2. Create UUID mappings for all integer IDs
3. Create and populate the DuckDB database tables
4. Generate proper normalized data for person, role, school, tournament, participant, match, and participant_match tables
"""

def run(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create DuckDB database from the processed CSV files.
    
    Args:
        df: The main matches dataframe (not used directly, we read from CSV files)
        
    Returns:
        df: The original dataframe (unchanged)
    """
    
    # Get the project root path
    project_root = Path(__file__).parent.parent.parent
    
    # Read the CSV files
    matches_csv_path = project_root / "output/create_participant_id_results.csv"
    participants_csv_path = project_root / "output/participant_id_df_results.csv"
    
    logging.info(f"Reading matches from: {matches_csv_path}")
    logging.info(f"Reading participants from: {participants_csv_path}")
    
    if not matches_csv_path.exists():
        raise FileNotFoundError(f"Matches CSV not found: {matches_csv_path}")
    if not participants_csv_path.exists():
        raise FileNotFoundError(f"Participants CSV not found: {participants_csv_path}")
    
    matches_df = pd.read_csv(matches_csv_path, low_memory=False)
    participants_df = pd.read_csv(participants_csv_path)
    
    logging.info(f"Loaded {len(matches_df)} matches and {len(participants_df)} participants")
    
    # Create the database
    db_path = project_root / "output/wrestling.duckdb"
    create_database(matches_df, participants_df, str(db_path))
    
    logging.info(f"Created DuckDB database at {db_path}")
    return df


def create_database(matches_df: pd.DataFrame, participants_df: pd.DataFrame, db_path: str):
    """Create and populate the DuckDB database."""
    
    # Delete existing database if it exists
    db_file = Path(db_path)
    if db_file.exists():
        db_file.unlink()
        logging.info(f"Deleted existing database: {db_path}")
    
    # Create UUID mappings for integer IDs
    uuid_mappings = create_uuid_mappings(matches_df, participants_df)
    
    # Connect to DuckDB
    conn = duckdb.connect(db_path)
    
    try:
        # Create tables
        create_tables(conn)
        
        # Populate tables in dependency order
        populate_person_table(conn, participants_df, uuid_mappings)
        populate_role_table(conn, participants_df, uuid_mappings)
        school_mapping = populate_school_table(conn, participants_df)
        tournament_mapping = populate_tournament_table(conn, matches_df)
        populate_participant_table(conn, participants_df, uuid_mappings, school_mapping)
        populate_match_table(conn, matches_df, uuid_mappings, tournament_mapping)
        populate_participant_match_table(conn, matches_df, uuid_mappings)
        
        logging.info("Database populated successfully")
        
    finally:
        conn.close()


def create_uuid_mappings(matches_df: pd.DataFrame, participants_df: pd.DataFrame) -> Dict[str, Dict[Any, str]]:
    """Create UUID mappings for all integer IDs."""
    
    mappings = {}
    
    # Get all unique wrestler_ids (person_ids) - handle both int and float
    wrestler_ids = set()
    wrestler_ids.update(participants_df['wrestler_id'].dropna().astype(int).unique())
    wrestler_ids.update(matches_df['w_wrestler_id'].dropna().astype(int).unique())
    wrestler_ids.update(matches_df['l_wrestler_id'].dropna().astype(int).unique())
    
    mappings['person_id'] = {wid: str(uuid.uuid4()) for wid in wrestler_ids}
    
    # Get all unique match_ids - handle both int and float
    match_ids = matches_df['match_id'].dropna().astype(int).unique()
    mappings['match_id'] = {mid: str(uuid.uuid4()) for mid in match_ids}
    
    # Schools - create UUIDs for unique school names
    schools = set()
    schools.update(participants_df['school'].dropna().unique())
    schools.update(matches_df['w_school'].dropna().unique())
    schools.update(matches_df['l_school'].dropna().unique())
    
    mappings['school_id'] = {school: str(uuid.uuid4()) for school in schools}
    
    # Tournaments - create UUIDs for unique year combinations (assuming one tournament per year)
    years = matches_df['year'].dropna().astype(int).unique()
    mappings['tournament_id'] = {year: str(uuid.uuid4()) for year in years}
    
    return mappings


def create_tables(conn):
    """Create all tables according to the schema."""
    
    # Define tables with VARCHAR instead of UUID for DuckDB compatibility
    tables_sql = """
    CREATE TABLE person (
      person_id VARCHAR PRIMARY KEY,
      first_name TEXT,
      last_name TEXT
    );

    CREATE TABLE role (
      role_id VARCHAR PRIMARY KEY,
      person_id VARCHAR REFERENCES person(person_id),
      role_type TEXT -- 'wrestler', 'coach', etc.
    );

    CREATE TABLE school (
        school_id VARCHAR PRIMARY KEY,
        name TEXT,
        location TEXT
    );

    CREATE TABLE tournament (
      tournament_id VARCHAR PRIMARY KEY,
      name TEXT,
      year INT,
      location TEXT
    );

    CREATE TABLE participant (
        participant_id VARCHAR PRIMARY KEY,
        role_id VARCHAR REFERENCES role(role_id),
        school_id VARCHAR REFERENCES school(school_id),
        year INT,
        weight_class TEXT,
        seed INT
    );

    CREATE TABLE match (
      match_id VARCHAR PRIMARY KEY,
      round TEXT,
      round_order INT,
      bracket_order INT,
      tournament_id VARCHAR REFERENCES tournament(tournament_id)
    );

    CREATE TABLE participant_match (
      match_id VARCHAR REFERENCES match(match_id),
      participant_id VARCHAR REFERENCES participant(participant_id),
      is_winner BOOLEAN,
      score INT,
      result_type TEXT, -- 'FALL', 'DEC', 'TECH', etc.
      fall_time VARCHAR,
      next_match_id VARCHAR REFERENCES match(match_id),
      PRIMARY KEY (match_id, participant_id)
    );
    """
    
    # Split and execute each CREATE TABLE statement
    statements = [stmt.strip() for stmt in tables_sql.split(';') if stmt.strip() and 'CREATE TABLE' in stmt]
    
    for statement in statements:
        try:
            conn.execute(statement)
            table_name = statement.split()[2]
            logging.info(f"Created table: {table_name}")
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            logging.error(f"Statement: {statement}")
            raise


def populate_person_table(conn, participants_df: pd.DataFrame, uuid_mappings: Dict[str, Dict[Any, str]]):
    """Populate the person table."""
    
    # Get unique persons based on wrestler_id (converted to int to avoid float issues)
    persons = []
    seen_wrestler_ids = set()
    
    for _, row in participants_df.iterrows():
        wrestler_id = row['wrestler_id']
        if pd.notna(wrestler_id):
            wrestler_id_int = int(wrestler_id)  # Convert to int consistently
            if wrestler_id_int not in seen_wrestler_ids:
                seen_wrestler_ids.add(wrestler_id_int)
                
                name_parts = row['name'].split('|') if '|' in str(row['name']) else [str(row['name'])]
                first_name = name_parts[0] if len(name_parts) > 0 else ''
                last_name = name_parts[1] if len(name_parts) > 1 else ''
                
                persons.append({
                    'person_id': uuid_mappings['person_id'][wrestler_id_int],
                    'first_name': first_name,
                    'last_name': last_name
                })
    
    logging.info(f"Prepared {len(persons)} unique person records")
    
    # Insert records one by one for better error handling
    for person in persons:
        try:
            conn.execute(
                "INSERT INTO person (person_id, first_name, last_name) VALUES (?, ?, ?)",
                [person['person_id'], person['first_name'], person['last_name']]
            )
        except Exception as e:
            logging.error(f"Error inserting person {person}: {e}")
            raise
    
    logging.info(f"Inserted {len(persons)} persons")


def populate_role_table(conn, participants_df: pd.DataFrame, uuid_mappings: Dict[str, Dict[Any, str]]):
    """Populate the role table."""
    
    # Create one role per person (all wrestlers for now)
    roles = []
    seen_wrestler_ids = set()
    role_id_mapping = {}  # Map wrestler_id to role_id
    
    for _, row in participants_df.iterrows():
        wrestler_id = row['wrestler_id']
        if pd.notna(wrestler_id):
            wrestler_id_int = int(wrestler_id)  # Convert to int consistently
            if wrestler_id_int not in seen_wrestler_ids:
                seen_wrestler_ids.add(wrestler_id_int)
                
                role_id = str(uuid.uuid4())
                roles.append({
                    'role_id': role_id,
                    'person_id': uuid_mappings['person_id'][wrestler_id_int],
                    'role_type': 'wrestler'
                })
                
                # Store role_id mapping for later use
                role_id_mapping[wrestler_id_int] = role_id
    
    # Store the role_id mapping in uuid_mappings for other functions to use
    uuid_mappings['role_id'] = role_id_mapping
    
    logging.info(f"Prepared {len(roles)} role records")
    
    # Insert records one by one for better error handling
    for role in roles:
        try:
            conn.execute(
                "INSERT INTO role (role_id, person_id, role_type) VALUES (?, ?, ?)",
                [role['role_id'], role['person_id'], role['role_type']]
            )
        except Exception as e:
            logging.error(f"Error inserting role {role}: {e}")
            raise
    
    logging.info(f"Inserted {len(roles)} roles")


def populate_school_table(conn, participants_df: pd.DataFrame):
    """Populate the school table."""
    
    # Get unique schools and create UUIDs
    unique_schools = participants_df['school'].dropna().unique()
    schools = []
    school_mapping = {}
    
    for school_name in unique_schools:
        school_id = str(uuid.uuid4())
        schools.append({
            'school_id': school_id,
            'name': school_name,
            'location': None  # We don't have location data
        })
        school_mapping[school_name] = school_id
    
    logging.info(f"Prepared {len(schools)} school records")
    
    # Insert records
    for school in schools:
        try:
            conn.execute(
                "INSERT INTO school (school_id, name, location) VALUES (?, ?, ?)",
                [school['school_id'], school['name'], school['location']]
            )
        except Exception as e:
            logging.error(f"Error inserting school {school}: {e}")
            raise
    
    logging.info(f"Inserted {len(schools)} schools")
    return school_mapping


def populate_tournament_table(conn, matches_df: pd.DataFrame):
    """Populate the tournament table."""
    
    # Get unique years and create tournaments
    unique_years = matches_df['year'].dropna().unique()
    tournaments = []
    tournament_mapping = {}
    
    for year in unique_years:
        tournament_id = str(uuid.uuid4())
        tournaments.append({
            'tournament_id': tournament_id,
            'name': f"NCAA Division I Wrestling Championships {int(year)}",
            'year': int(year),
            'location': None  # We don't have location data
        })
        tournament_mapping[int(year)] = tournament_id
    
    logging.info(f"Prepared {len(tournaments)} tournament records")
    
    # Insert records
    for tournament in tournaments:
        try:
            conn.execute(
                "INSERT INTO tournament (tournament_id, name, year, location) VALUES (?, ?, ?, ?)",
                [tournament['tournament_id'], tournament['name'], tournament['year'], tournament['location']]
            )
        except Exception as e:
            logging.error(f"Error inserting tournament {tournament}: {e}")
            raise
    
    logging.info(f"Inserted {len(tournaments)} tournaments")
    return tournament_mapping


def populate_participant_table(conn, participants_df: pd.DataFrame, uuid_mappings: Dict[str, Dict[Any, str]], school_mapping: Dict[str, str]):
    """Populate the participant table."""
    
    participants = []
    for _, row in participants_df.iterrows():
        wrestler_id = row['wrestler_id']
        if pd.notna(wrestler_id):
            wrestler_id_int = int(wrestler_id)  # Convert to int consistently
            
            # Get role_id for this wrestler
            role_id = uuid_mappings.get('role_id', {}).get(wrestler_id_int)
            if not role_id:
                logging.warning(f"No role_id found for wrestler_id {wrestler_id_int}")
                continue
                
            participants.append({
                'participant_id': row['participant_id'],  # Already a UUID
                'role_id': role_id,
                'school_id': school_mapping.get(row['school']),
                'year': int(row['year']) if pd.notna(row['year']) else None,
                'weight_class': str(row['weight']) if pd.notna(row['weight']) else None,
                'seed': int(row['seed']) if pd.notna(row['seed']) and row['seed'] != 0 else None
            })
    
    logging.info(f"Prepared {len(participants)} participant records")
    
    # Insert records
    for participant in participants:
        try:
            conn.execute(
                "INSERT INTO participant (participant_id, role_id, school_id, year, weight_class, seed) VALUES (?, ?, ?, ?, ?, ?)",
                [participant['participant_id'], participant['role_id'], participant['school_id'], 
                 participant['year'], participant['weight_class'], participant['seed']]
            )
        except Exception as e:
            logging.error(f"Error inserting participant {participant}: {e}")
            raise
    
    logging.info(f"Inserted {len(participants)} participants")


def populate_match_table(conn, matches_df: pd.DataFrame, uuid_mappings: Dict[str, Dict[Any, str]], tournament_mapping: Dict[int, str]):
    """Populate the match table."""
    
    matches = []
    for _, row in matches_df.iterrows():
        match_id = row['match_id']
        if pd.notna(match_id):
            year = int(row['year']) if pd.notna(row['year']) else None
            tournament_id = tournament_mapping.get(year) if year else None
            
            matches.append({
                'match_id': uuid_mappings['match_id'][int(match_id)],
                'round': row['round'] if pd.notna(row['round']) else None,
                'round_order': None,  # We don't have this data
                'bracket_order': None,  # We don't have this data
                'tournament_id': tournament_id
            })
    
    logging.info(f"Prepared {len(matches)} match records")
    
    # Insert records
    for match in matches:
        try:
            conn.execute(
                "INSERT INTO match (match_id, round, round_order, bracket_order, tournament_id) VALUES (?, ?, ?, ?, ?)",
                [match['match_id'], match['round'], match['round_order'], match['bracket_order'], match['tournament_id']]
            )
        except Exception as e:
            logging.error(f"Error inserting match {match}: {e}")
            raise
    
    logging.info(f"Inserted {len(matches)} matches")


def populate_participant_match_table(conn, matches_df: pd.DataFrame, uuid_mappings: Dict[str, Dict[Any, str]]):
    """Populate the participant_match table."""
    
    # First, get all valid participant IDs from the participant table
    valid_participant_ids = set()
    try:
        result = conn.execute("SELECT participant_id FROM participant").fetchall()
        valid_participant_ids = {row[0] for row in result}
        logging.info(f"Found {len(valid_participant_ids)} valid participant IDs")
    except Exception as e:
        logging.error(f"Error getting participant IDs: {e}")
        raise
    
    participant_matches = []
    skipped_count = 0
    
    for _, row in matches_df.iterrows():
        match_id = row['match_id']
        if pd.notna(match_id):
            match_uuid = uuid_mappings['match_id'][int(match_id)]
            
            # Winner participant
            w_participant_id = row['w_participant_id']
            if pd.notna(w_participant_id) and w_participant_id in valid_participant_ids:
                participant_matches.append({
                    'match_id': match_uuid,
                    'participant_id': w_participant_id,
                    'is_winner': True,
                    'score': int(row['w_score']) if pd.notna(row['w_score']) else None,
                    'result_type': row['type'] if pd.notna(row['type']) else None,
                    'fall_time': format_fall_time(row) if str(row.get('type', '')).lower() == 'fall' else None,
                    'next_match_id': None  # We don't have this data yet
                })
            elif pd.notna(w_participant_id):
                skipped_count += 1
                logging.warning(f"Skipping winner participant_id {w_participant_id} - not found in participant table")
            
            # Loser participant
            l_participant_id = row['l_participant_id']
            if pd.notna(l_participant_id) and l_participant_id in valid_participant_ids:
                participant_matches.append({
                    'match_id': match_uuid,
                    'participant_id': l_participant_id,
                    'is_winner': False,
                    'score': int(row['l_score']) if pd.notna(row['l_score']) else None,
                    'result_type': row['type'] if pd.notna(row['type']) else None,
                    'fall_time': None,  # Only winner gets fall time
                    'next_match_id': None  # We don't have this data yet
                })
            elif pd.notna(l_participant_id):
                skipped_count += 1
                logging.warning(f"Skipping loser participant_id {l_participant_id} - not found in participant table")
    
    logging.info(f"Prepared {len(participant_matches)} participant_match records")
    if skipped_count > 0:
        logging.warning(f"Skipped {skipped_count} participant_match records due to missing participant IDs")
    
    # Insert records
    for pm in participant_matches:
        try:
            conn.execute(
                "INSERT INTO participant_match (match_id, participant_id, is_winner, score, result_type, fall_time, next_match_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [pm['match_id'], pm['participant_id'], pm['is_winner'], pm['score'], pm['result_type'], pm['fall_time'], pm['next_match_id']]
            )
        except Exception as e:
            logging.error(f"Error inserting participant_match {pm}: {e}")
            raise
    
    logging.info(f"Inserted {len(participant_matches)} participant_match records")


def format_fall_time(row) -> str:
    """Format fall time as a time string."""
    min_val = row.get('min')
    sec_val = row.get('sec')
    
    # Handle NaN and None values
    if pd.isna(min_val) or min_val is None:
        min_val = 0
    if pd.isna(sec_val) or sec_val is None:
        sec_val = 0
    
    # Convert to int safely
    try:
        min_val = int(float(min_val))
        sec_val = int(float(sec_val))
    except (ValueError, TypeError):
        return None
    
    if min_val or sec_val:
        return f"{min_val:02d}:{sec_val:02d}"
    return None
