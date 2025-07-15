import logging
import json
import pandas as pd
from pathlib import Path
from .steps import clean

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Pipeline:
    STEPS = [
        "clean",
        # "process_data",
        # "save_results"
    ]

    def __init__(self, input_path: Path, output_dir: Path, state_path: Path = Path("logs/pipeline_state.json")):
        self.input_path = input_path
        self.output_dir = output_dir
        self.state_path = state_path
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()
        self.logger = logger

    @classmethod
    def from_saved_state(cls):
        state = cls.load_state_file()
        return cls(Path(state['input_path']), Path(state['output_dir']), Path(state['state_path']))


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
            "input_path": str(self.input_path),
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
            "input_path": str(self.input_path),
            "output_dir": str(self.output_dir),
            "state_path": str(self.state_path)
        }
        
    def save_results(self, df: pd.DataFrame, step_name: str):
        output_file = self.output_dir / f"{step_name}_results.csv"
        df.to_csv(output_file, index=False)
        self.logger.info(f"Results saved to {output_file}")

    def run(self):
        df = pd.read_csv(self.input_path)

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


    def step_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        return clean.run(df)