import logging
import json
import pandas as pd
from pathlib import Path
from .steps import prep, drop_cols, convert_to_csv, create_name, manual_edits, create_participant_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data/"
# RAW_FILE = "raw_data.xlsx"

class Pipeline:
    STEPS = [
        "convert_to_csv",
        "prep",
        "drop_cols",
        "manual_edits",
        "create_name",
        "create_participant_id"
    ]

    def __init__(self, input_file: Path, output_dir: Path, state_path: Path = Path("logs/pipeline_state.json")):
        self.input_file = DATA_DIR / input_file
        self.output_dir = PROJECT_ROOT / output_dir
        self.state_path = state_path
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()
        self.logger = logger

    @classmethod
    def from_saved_state(cls):
        state = cls.load_state_file()
        return cls(Path(state['input_file']), Path(state['output_dir']), Path(state['state_path']))


    @staticmethod
    def load_state_file(state_path: Path = Path("logs/pipeline_state.json")):
        if state_path.exists():
            with open(state_path, 'r') as f:
                return json.load(f)
        return {}
    
    def _load_state(self):
        if self.state_path.exists():
            return json.loads(self.state_path.read_text())
        return {
            "last_completed_step": None,
            "input_file": str(self.input_file),
            "output_dir": str(self.output_dir),
            "state_path": str(self.state_path)
        }
    
    def _save_state(self, step_name: str):
        self.state['last_completed_step'] = step_name
        self.state_path.write_text(json.dumps(self.state, indent=2))

    def reset_state(self):
        """Reset the pipeline state by deleting the state file."""
        if self.state_path.exists():
            self.state_path.unlink()
        self.state = {
            "last_completed_step": None,
            "input_file": str(self.input_file),
            "output_dir": str(self.output_dir),
            "state_path": str(self.state_path)
        }

    def save_results(self, df: pd.DataFrame, step_name: str):
        output_file = self.output_dir / f"{step_name}_results.csv"
        df.to_csv(output_file, index=False)
        self.logger.info(f"Results saved to {output_file}")

    def run(self):
        self.logger.info(f"Starting pipeline with input: {self.input_file} and output: {self.output_dir}")
        df = pd.read_excel(self.input_file)

        start_index = 0
        if self.state['last_completed_step']:
            start_index = self.STEPS.index(self.state['last_completed_step']) + 1

        for step_name in self.STEPS[start_index:]:
            self.logger.info(f"Running step: {step_name}")
            step_func = getattr(self, f"step_{step_name}", None)
            if not step_func:
                raise ValueError(f"Step '{step_name}' not implemented in Pipeline class.")
            try:
                df = step_func(df)
            except Exception as e:
                self.logger.error(f"Error in step '{step_name}': {e}")
            if df is None:
                self.logger.error(f"Step '{step_name}' returned None, stopping pipeline.")
                break
            self.save_results(df, step_name)
            self._save_state(step_name)

    def step_convert_to_csv(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert input data to CSV format."""
        convert_to_csv.run(df)
        return df

    def step_prep(self, df: pd.DataFrame) -> pd.DataFrame:
        """Perform initial cleaning steps on the DataFrame."""
        return prep.run(df)
    
    def step_drop_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop unnecessary columns from the DataFrame."""
        return drop_cols.run(df)

    def step_create_name(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create name columns by concatenating first and last names."""
        return create_name.run(df)
    
    def step_manual_edits(self, df: pd.DataFrame) -> pd.DataFrame:
        """Perform manual edits on the DataFrame."""
        return manual_edits.run(df)
    
    def step_create_participant_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create unique participant IDs for each wrestler."""
        df, part_df = create_participant_id.run(df)
        self.save_results(part_df, "participant_id_df")
        return df