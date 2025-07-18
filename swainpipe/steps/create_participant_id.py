import logging
import json
import uuid
import pandas as pd

"""
The raw data is at the 1 match per row level. Each row represents a unique match between two wrestlers.
Our ultimate goal is to create a unique person_id for each unique person in the database.
(we will also have a wrestler_id for each wrestler, but that is not the same as a person_id).

Becasue the raw data is messy, we are going to start with creating a unique participant_id for each wrestler.
This will be a UUID that is generated for each wrestler in an NCAA tournament.

A participant_id is unique for each name, weight, year, school combination.

The data has attributes like:
- w_first: First name of the wrestler in the winning wrestler
- w_last: Last name of the wrestler in the winning wrestler
- l_first: First name of the wrestler in the losing wrestler
- l_last: Last name of the wrestler in the losing wrestler
- w_school: School of the wrestler in the winning wrestler
- l_school: School of the wrestler in the losing wrestler
- weight: Weight class of the match
- year: Year of the match
- match_id: Unique identifier for the match

We need to create a union of w_first, w_last, w_school, weight, year and l_first, l_last, l_school, weight, year
to create a unique participant_id for each wrestler.

with _union as (
    select distinct w_name as name, w_school as school, weight, year from raw_data
    union
    select distinct l_name as name, l_school as school, weight, year from raw_data
)
select distinct name, school, weight, year
from _union
"""

def run(df: pd.DataFrame) -> pd.DataFrame:
    participant_df = create_participant_df(df)
    participant_df['participant_id'] = [str(uuid.uuid4()) for _ in range(len(participant_df))]
    logging.info(f"Created {len(participant_df)} unique participant IDs.")
    # Merge participant_id for winning wrestler
    df = df.merge(
        participant_df[['name', 'school', 'weight', 'year', 'participant_id']].rename(
            columns={
                'name': 'w_name',
                'school': 'w_school',
                'participant_id': 'w_participant_id'
            }
        ),
        on=['w_name', 'w_school', 'weight', 'year'],
        how='left'
    )

    # Merge participant_id for losing wrestler
    df = df.merge(
        participant_df[['name', 'school', 'weight', 'year', 'participant_id']].rename(
            columns={
                'name': 'l_name',
                'school': 'l_school',
                'participant_id': 'l_participant_id'
            }
        ),
        on=['l_name', 'l_school', 'weight', 'year'],
        how='left'
    )
    return df, participant_df


def create_participant_df(df: pd.DataFrame) -> pd.DataFrame:
    w_union = df[['w_name', 'w_school', 'weight', 'year', 'w_seed', 'w_wrestler_id']].drop_duplicates()
    l_union = df[['l_name', 'l_school', 'weight', 'year', 'l_seed', 'l_wrestler_id']].drop_duplicates()
    w_union = w_union.rename(columns={'w_name': 'name', 'w_school': 'school', 'w_seed': 'seed', 'w_wrestler_id': 'wrestler_id'})
    l_union = l_union.rename(columns={'l_name': 'name', 'l_school': 'school', 'l_seed': 'seed', 'l_wrestler_id': 'wrestler_id'})
    return pd.concat([w_union, l_union], ignore_index=True).drop_duplicates()