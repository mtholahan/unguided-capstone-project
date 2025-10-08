"""
QA_config_usage_audit.py
-------------------------------------------------------------
Scans your script corpus to report whether each constant in
config.py is actually referenced.

Enhancements:
  • Groups unused constants by category suffix (e.g., _PATH, _LIMIT)
  • Color-coded console output (green = used, red = unused)
  • Writes CSV to <output_dir>/config_usage_audit.csv

Usage:
    python QA_config_usage_audit.py \
        [--scripts-dir <scripts>] [--output-dir <out>] [--config-file <config.py>]
"""

import ast
import re
import argparse
import pandas as pd
from pathlib import Path
from collections import defaultdict

try:
    from colorama import Fore, Style, init
    init(autoreset=True)
except Exception:
    class _Dummy:
        RED = GREEN = CYAN = YELLOW = RESET_ALL = ""
    Fore = Style = _Dummy()


def default_paths():
    here = Path(__file__).resolve().parent
    scripts_dir = here
    out_dir = scripts_dir / "audit_reports"
    return scripts_dir, out_dir


def extract_config_constants(config_file: Path):
    constants = {}
    text = config_file.read_text(encoding="utf-8")
    tree = ast.parse(text)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id.isupper():
                    val = getattr(node.value, "value", None)
                    constants[target.id] = val
    return constants


def scan_scripts_for_constants(constants: dict, scripts_dir: Path, exclude: set):
    results = []
    for path in scripts_dir.glob("*.py"):
        if path.name in exclude:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for const in constants.keys():
            pattern = rf"\b(config\.)?{re.escape(const)}\b"
            hits = re.findall(pattern, text)
            if hits:
                results.append({"constant": const, "file": path.name, "count": len(hits)})
    return pd.DataFrame(results)


def summarize_usage(df: pd.DataFrame, constants: dict):
    all_consts = pd.DataFrame({"constant": list(constants.keys())})
    if df.empty:
        usage = all_consts.copy()
        usage["count"] = 0
    else:
        usage = all_consts.merge(df.groupby("constant")["count"].sum().reset_index(), on="constant", how="left").fillna(0)
    usage["count"] = usage["count"].astype(int)
    usage["in_use"] = usage["count"] > 0
    usage.sort_values(by=["in_use", "constant"], ascending=[False, True], inplace=True)
    return usage


def categorize_constant(name: str) -> str:
    name = name.upper()
    if name.endswith("_PATH"):
        return "Paths"
    if name.endswith("_DIR") or name.endswith("_FOLDER"):
        return "Directories"
    if name.endswith("_LIMIT"):
        return "Limits"
    if name.endswith("_THRESHOLD"):
        return "Thresholds"
    if name.endswith("_SECONDS") or name.endswith("_TIMEOUT"):
        return "Timing"
    if name.endswith("_URL"):
        return "URLs"
    if name.endswith("_FILE"):
        return "Files"
    if name.endswith("_RATE"):
        return "Rates"
    if name.endswith("_COUNT") or name.endswith("_NUM"):
        return "Counts"
    if name.endswith("_FLAG") or name.endswith("_MODE"):
        return "Flags / Modes"
    return "Other"


def main():
    parser = argparse.ArgumentParser(description="Audit config constant usage across scripts")
    scripts_dir_default, out_dir_default = default_paths()
    parser.add_argument("--scripts-dir", type=str, default=str(scripts_dir_default))
    parser.add_argument("--output-dir", type=str, default=str(out_dir_default))
    parser.add_argument("--config-file", type=str, default=str((scripts_dir_default / "config.py")))
    args = parser.parse_args()

    scripts_dir = Path(args.scripts_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    config_file = Path(args.config_file).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"{Fore.CYAN}🔍 Scanning for config constant usage across scripts…{Style.RESET_ALL}")
    constants = extract_config_constants(config_file)
    print(f"📦 Found {len(constants)} constants in {config_file.name}")

    exclude = {
        "QA_scan_magic_literals.py",
        "QA_config_reconciler_numbers.py",
        "QA_config_usage_audit.py",
        "QA_analyze_suggestions_clusters.py",
    }

    df_hits = scan_scripts_for_constants(constants, scripts_dir, exclude)
    summary = summarize_usage(df_hits, constants)
    summary["category"] = summary["constant"].apply(categorize_constant)

    csv_path = output_dir / "config_usage_audit.csv"
    summary.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"📊 Wrote usage summary → {csv_path}")

    used = int(summary["in_use"].sum())
    unused = int(len(summary) - used)
    print(f"\n📈 {Fore.GREEN}{used} constants referenced{Style.RESET_ALL}, {Fore.RED}{unused} unused{Style.RESET_ALL}.\n")

    unused_df = summary[~summary["in_use"]]
    if unused_df.empty:
        print(f"{Fore.GREEN}✅ All constants are in use!{Style.RESET_ALL}")
    else:
        print(f"{Fore.YELLOW}🔹 Unused constants by category:{Style.RESET_ALL}")
        for category in sorted(unused_df["category"].unique()):
            print(f"\n  {Fore.CYAN}{category}{Style.RESET_ALL}")
            for const in sorted(unused_df.loc[unused_df["category"] == category, "constant"].tolist()):
                print(f"     {Fore.RED}{const}{Style.RESET_ALL}")

    print("\n💡 Tip: Remove or repurpose unused constants; keep config.py lean.")


if __name__ == "__main__":
    main()