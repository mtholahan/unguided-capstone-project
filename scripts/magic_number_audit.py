#!/usr/bin/env python3
r"""
magic_number_audit.py
Improved lightweight auditor for Step 5 ETL scope.

Scans Python files for "magic numbers" or hardcoded literals,
excluding known-safe values and utility scripts.

Usage:
    python Scripts/magic_number_audit.py
    python Scripts/magic_number_audit.py --root .\Scripts --export ..\logs\audit.md
"""

import argparse
import ast
import datetime
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

# ------------------------------------------------------------
# AST Utility Helpers
# ------------------------------------------------------------
def get_parent_function_name(node: ast.AST) -> Optional[str]:
    """Walks up the AST tree to find the nearest function call name, if any."""
    parent = getattr(node, "parent", None)
    while parent:
        if isinstance(parent, ast.Call):
            func = parent.func
            if isinstance(func, ast.Attribute):
                return func.attr  # e.g., logger.info -> 'info'
            elif isinstance(func, ast.Name):
                return func.id   # e.g., print("...") -> 'print'
        parent = getattr(parent, "parent", None)
    return None


# ------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------
def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="latin-1")


def load_existing_config_values(config_path: Optional[Path]) -> Dict[str, Any]:
    """Extracts uppercase constant values from config.py."""
    values: Dict[str, Any] = {}
    if not config_path or not config_path.exists():
        return values

    try:
        tree = ast.parse(read_text(config_path))
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id.isupper():
                        try:
                            values[target.id] = ast.literal_eval(node.value)
                        except Exception:
                            continue
    except Exception as e:
        print(f"[WARN] Could not parse {config_path}: {e}")
    return values


# ------------------------------------------------------------
# Issue model
# ------------------------------------------------------------
class Issue:
    def __init__(self, file: Path, line: int, kind: str, value: Any, suggestion: str):
        self.file = file
        self.line = line
        self.kind = kind
        self.value = value
        self.suggestion = suggestion


# ------------------------------------------------------------
# AST Visitor
# ------------------------------------------------------------
class MagicFinder(ast.NodeVisitor):
    """AST walker that finds literal constants not in config.py."""

    def __init__(self, file: Path, config_literals: Dict[Any, str]):
        self.file = file
        self.config_literals = config_literals
        self.issues: List[Issue] = []

    def _report(self, kind: str, value: Any, suggestion: str, node: ast.AST):
        """Append a new Issue to the findings list with standard metadata."""
        self.issues.append(
            Issue(
                file=self.file,
                line=getattr(node, "lineno", 0),
                kind=kind,
                value=value,
                suggestion=suggestion,
            )
        )

    def visit_Constant(self, node: ast.Constant):
        """
        Detect literal constants that may represent magic numbers, paths, or files.
        Skips harmless or non-semantic strings like docstrings, logs, and messages.
        """
        val = node.value

        # Skip harmless primitive constants
        if isinstance(val, (bool, type(None))):
            return

        # --------------------------------------------------
        # STRING FILTERS
        # --------------------------------------------------
        if isinstance(val, str):
            # Skip multiline or long docstrings
            if "\n" in val or len(val) > 200:
                return

            # Context check → skip if inside a print() or logger.*() call
            func_name = get_parent_function_name(node)
            if func_name and func_name.lower() in {
                "print",
                "debug",
                "info",
                "warning",
                "error",
                "critical",
                "exception",
                "log",
            }:
                return  # skip logging and print message strings

            # Skip regex patterns
            if re.search(r"\\[wdsb]", val):
                return

            # Paths / URLs / filenames still count as findings
            if re.match(r"https?://", val):
                self._report("url", val, "API_BASE_URL", node)
            elif re.search(r"/|\\\\", val):
                self._report("path", val, "PATH_VALUE", node)
            elif re.search(r"\.(csv|tsv|parquet|txt)$", val, re.I):
                self._report("file", val, "FILENAME_CONST", node)
            return


        # --------------------------------------------------
        # NUMERIC FILTERS
        # --------------------------------------------------
        if isinstance(val, (int, float)):
            # Ignore trivial constants (0, ±1)
            if val in (0, 1, -1):
                return

            # Skip time.sleep-like durations (common in debug)
            if 0 < val < 1e-2:
                # Too small → probably a tolerance, still record
                self._report("number", val, "CONST_VALUE", node)
                return

            # Record any numeric literal not found in config
            if val not in self.config_literals:
                self._report("number", val, "CONST_VALUE", node)


# ------------------------------------------------------------
# Main logic
# ------------------------------------------------------------
def scan_file(file: Path, config_values: Dict[str, Any]) -> List[Issue]:
    try:
        code = read_text(file)
        tree = ast.parse(code)

        # attach parent references for context-aware filtering
        for parent in ast.walk(tree):
            for child in ast.iter_child_nodes(parent):
                child.parent = parent

    except Exception as e:
        print(f"[WARN] Skipping unparsable file: {file.name} ({e})")
        return []

    literals = {
        v: k for k, v in config_values.items()
        if isinstance(v, (int, float, str))
    }
    finder = MagicFinder(file, literals)
    finder.visit(tree)

    return finder.issues   # ✅ this line ensures we always return a list



def write_report(issues: List[Issue], report_path: Path, root: Path):
    """
    Writes Markdown + CSV reports.
    Adds exact and approximate mapping to config.py constants.
    """
    import csv
    from math import isclose

    report_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path = report_path.with_suffix(".csv")

    # Load config constants
    config_path = Path(root, "config.py")
    config_constants = load_existing_config_values(config_path)

    # Filter to hashable (simple) types only for exact matching
    literal_to_const = {}
    for k, v in config_constants.items():
        try:
            hash(v)  # test if hashable
            literal_to_const[v] = k
        except TypeError:
            continue  # skip lists/dicts/unhashables

    # For approximate matching: numeric constants only
    numeric_constants = {
        k: v for k, v in config_constants.items() if isinstance(v, (int, float))
    }

    # For approximate matching: keep numeric constants only
    numeric_constants = {
        k: v for k, v in config_constants.items() if isinstance(v, (int, float))
    }

    # Helper: find approximate match for numeric values
    def find_approx_match(value):
        if not isinstance(value, (int, float)):
            return "", ""
        closest = None
        min_diff = float("inf")
        for const_name, const_val in numeric_constants.items():
            try:
                diff = abs(const_val - value)
                if diff < min_diff:
                    min_diff = diff
                    closest = const_name
            except Exception:
                continue
        if closest and isclose(value, numeric_constants[closest], rel_tol=0.05, abs_tol=1e-3):
            delta = round(value - numeric_constants[closest], 6)
            return closest, f"{delta:+}"
        return "", ""

    # Build summary
    summary: Dict[str, int] = {}
    for i in issues:
        summary[i.kind] = summary.get(i.kind, 0) + 1

    # Markdown Report
    with report_path.open("w", encoding="utf-8") as f:
        f.write(f"# Step 5 Magic Number Audit\n_Generated: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}_\n\n")
        f.write(f"**Root:** `{root}`  \n**Total Findings:** {len(issues)}\n\n")

        if not issues:
            f.write("✅ No issues found.\n")
        else:
            f.write("### Summary by Kind\n")
            for kind, count in summary.items():
                f.write(f"- **{kind}** → {count}\n")
            f.write("\n---\n\n")
            f.write("| File | Line | Kind | Value | ConfigConstant | ApproxMatch | Δ | Suggestion |\n|---|---:|---|---|---|---|---|---|\n")
            for i in sorted(issues, key=lambda x: (str(x.file), i.line)):
                rel = i.file.relative_to(root)
                mapped = literal_to_const.get(i.value, "")
                approx, delta = find_approx_match(i.value)
                f.write(f"| `{rel}` | {i.line} | {i.kind} | `{i.value}` | `{mapped}` | `{approx}` | `{delta}` | `{i.suggestion}` |\n")

    # CSV Report
    with csv_path.open("w", encoding="utf-8", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["File", "Line", "Kind", "Value", "ConfigConstant", "ApproxMatch", "Delta", "Suggestion"])
        for i in sorted(issues, key=lambda x: (str(x.file), i.line)):
            rel = i.file.relative_to(root)
            mapped = literal_to_const.get(i.value, "")
            approx, delta = find_approx_match(i.value)
            writer.writerow([str(rel), i.line, i.kind, str(i.value), mapped, approx, delta, i.suggestion])

     # ----- Post-processing summary -----
    total = len(issues)
    exact = 0
    approx = 0

    for i in issues:
        mapped = literal_to_const.get(i.value, "")
        if mapped:
            exact += 1
        elif isinstance(i.value, (int, float)):
            approx_name, _ = find_approx_match(i.value)
            if approx_name:
                approx += 1

    unmapped = total - (exact + approx)
    coverage = (exact + approx) / total * 100 if total else 100

    print("\n[SUMMARY]")
    print(f"  Total findings     : {total}")
    print(f"  Exact config matches: {exact}")
    print(f"  Approx matches      : {approx}")
    print(f"  Unmapped literals   : {unmapped}")
    print(f"  Mapping coverage    : {coverage:.1f}%")

    print(f"\n[OK] Wrote audit: {len(issues)} findings → {report_path}")
    print(f"[OK] CSV export with config mapping and approximate matches → {csv_path}")


def main():
    parser = argparse.ArgumentParser(description="Audit for magic numbers and string literals in project source.")
    parser.add_argument("--root", default="Scripts", help="Root directory to scan (default: Scripts)")
    parser.add_argument("--config", default="Scripts/config.py", help="Path to config file (default: Scripts/config.py)")
    parser.add_argument("--report", default="logs/magic_number_audit.md", help="Output report path")
    parser.add_argument("--summary", action="store_true", help="Print summary table after scan")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    config_values = load_existing_config_values(Path(args.config))
    issues: List[Issue] = []

    excluded_patterns = re.compile(r"(venv|\.venv|archive|__pycache__|tests|ipynb_checkpoints)")
    skip_names = {
    "magic_number_audit.py",
    "step_99_ScratchPad.py",
    "utils.py",
    "config.py",  # ✅ exclude constant definitions
}

    for path in root.rglob("*.py"):
        if path.name in skip_names or excluded_patterns.search(str(path)):
            continue
        if re.search(r"step_0[7-9]|step_10", path.name):
            continue
        print(f"Scanning: {path.name}")
        issues.extend(scan_file(path, config_values))

    report_path = Path(args.report).resolve()
    write_report(issues, report_path, root)

    if args.summary:
        kinds = {}
        for i in issues:
            kinds[i.kind] = kinds.get(i.kind, 0) + 1
        print("\nSummary:")
        for k, v in kinds.items():
            print(f"  {k:10s}: {v}")
        print(f"\nTotal issues: {len(issues)}")


if __name__ == "__main__":
    main()
