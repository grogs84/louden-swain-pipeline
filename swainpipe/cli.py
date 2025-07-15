import typer
from pathlib import Path
from .pipeline import Pipeline

app = typer.Typer()

@app.command()
def run(
    input: Path = typer.Option(..., "--input", "-i", help="Input file path"),
    output: Path = typer.Option(..., "--output", "-o", help="Output directory path")
):
    pipeline = Pipeline(input, output)
    pipeline.run()

@app.command()
def resume():
    pipeline = Pipeline.from_saved_state()
    pipeline.run()

@app.command()
def status():
    state = Pipeline.load_state_file()
    typer.echo(f"Pipeline status: {state}")

@app.command()
def reset(
    state_file: Path = typer.Option(Path("logs/pipeline_state.json"), "--state-file", "-s", help="Path to state file to reset")
):
    """Reset pipeline state by deleting the state file."""
    if state_file.exists():
        state_file.unlink()
        typer.echo(f"✅ State file '{state_file}' deleted successfully")
    else:
        typer.echo(f"ℹ️  State file '{state_file}' doesn't exist - nothing to reset")
