import logging
import pandas as pd
from pathlib import Path

"""
this is actually a dummy step for converting to CSV. After each step the data is already in CSV format.
This step is to be explicit about the data format in the pipeline. Our raw data is in Excel format, but we convert it to CSV after the first step.
This is useful for ensuring that the data is in a consistent format for subsequent steps.
"""


def run(df: pd.DataFrame) -> pd.DataFrame:
    """Convert input DataFrame to CSV format."""
    logging.info(f"Converted input data to CSV")
    return df