import subprocess, pandas as pd, argparse, time
from pathlib import Path
from config import TMDB_DIR, SCRIPTS_PATH, METRICS_DIR

# --- CLI Argument Parser ---
def parse_args():
    parser = argparse.ArgumentParser(description="Compare original vs instrumented Step 08 match scripts.")
    parser.add_argument("--sample", type=int, default=None, help="Number of TMDB rows to sample for faster test runs.")
    return parser.parse_args()

def run_step(script_path: Path, sample=None):
    print(f"\n🚀 Running {script_path.name} ...")
    cmd = ["python", str(script_path)]
    if sample:
        cmd += ["--sample", str(sample)]

    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    duration = time.time() - start

    print(result.stdout)
    if result.returncode != 0:
        print(f"❌ Error in {script_path.name}:\n{result.stderr}")

    metrics_file = METRICS_DIR / "pipeline_metrics.csv"
    if not metrics_file.exists():
        metrics_file = TMDB_DIR / "pipeline_metrics.csv"

    if metrics_file.exists():
        df = pd.read_csv(metrics_file)
        df_recent = df.tail(1).to_dict('records')[0]
        df_recent['runtime_seconds'] = round(duration, 2)
        return df_recent
    else:
        return {'runtime_seconds': round(duration, 2)}

def main():
    args = parse_args()
    sample = args.sample

    original_script = SCRIPTS_PATH / "step_08_match_tmdb.py"
    instrumented_script = SCRIPTS_PATH / "step_08_match_instrumented.py"

   # metrics_original = run_step(original_script, sample=sample)
    metrics_instrumented = run_step(instrumented_script, sample=sample)

    # print("\n📊 Comparison Summary:")
    # comparison = pd.DataFrame([metrics_original, metrics_instrumented], index=['Original','Instrumented']).T
    # print(comparison)

    # comparison_path = TMDB_DIR / "step_08_comparison_metrics.csv"
    # comparison.to_csv(comparison_path)
    # print(f"\n✅ Comparison metrics saved to {comparison_path}")

if __name__ == "__main__":
    main()