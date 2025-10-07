#!/usr/bin/env python3
r"""
magic_number_audit_step5.py
Lightweight version of magic number auditor for Step 5 ETL scope.
Automatically skips itself, regex-heavy utils, and docstrings.

Usage:
    python magic_number_audit_step5.py --root C:\\Projects\\unguided-capstone-project\\Scripts --config .\\config.py --report ..\\logs\\magic_number_audit_step5.md
"""

import argparse
import ast
import os
import re
import sys
import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def read_text(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return p.read_text(encoding="latin-1")

def slugify(name: str) -> str:
    name = re.sub(r'[^A-Za-z0-9]+', '_', name).strip('_')
    return name.upper() or "CONST_VALUE"

def load_existing_config_values(config_path: Optional[Path]) -> Dict[str, Any]:
    values = {}
    if config_path and config_path.exists():
        try:
            tree = ast.parse(read_text(config_path))
            for node in tree.body:
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name) and target.id.isupper():
                            try:
                                values[target.id] = ast.literal_eval(node.value)
                            except Exception:
                                pass
        except Exception:
            pass
    return values

# ------------------------------------------------------------
# Visitor
# ------------------------------------------------------------

class Issue:
    def __init__(self, file: Path, line: int, kind: str, value: Any, suggestion: str):
        self.file = file
        self.line = line
        self.kind = kind
        self.value = value
        self.suggestion = suggestion

class MagicFinder(ast.NodeVisitor):
    def __init__(self, file: Path, config_literals: Dict[Any, List[str]]):
        self.file = file
        self.config_literals = config_literals
        self.issues: List[Issue] = []

    def visit_Constant(self, node: ast.Constant):
        val = node.value
        if isinstance(val, (bool, type(None))):
            return

        # Skip large docstrings or regex patterns
        if isinstance(val, str):
            if "\n" in val or re.search(r"\\\\[wdsb]", val):
                return
            if re.match(r"https?://", val):
                self.issues.append(Issue(self.file, node.lineno, "url", val, "API_BASE_URL"))
            elif re.search(r"/|\\\\", val):
                self.issues.append(Issue(self.file, node.lineno, "path", val, "PATH_VALUE"))
            elif re.search(r"\.csv$|\.tsv$|\.parquet$|\.txt$", val):
                self.issues.append(Issue(self.file, node.lineno, "file", val, "FILENAME_CONST"))
            return

        if isinstance(val, (int, float)) and val not in (0, 1, -1):
            if val not in self.config_literals:
                self.issues.append(Issue(self.file, node.lineno, "number", val, "CONST_VALUE"))

# ------------------------------------------------------------
# Main Logic
# ------------------------------------------------------------

def scan_file(file: Path, config_values: Dict[str, Any]) -> List[Issue]:
    code = read_text(file)
    tree = ast.parse(code)
    literals = {v: k for k, v in config_values.items() if isinstance(v, (int, float, str))}
    finder = MagicFinder(file, literals)
    finder.visit(tree)
    return finder.issues

def write_report(issues: List[Issue], report_path: Path, root: Path):
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as f:
        f.write(f"# Step 5 Magic Number Audit\n_Generated: {datetime.datetime.now().isoformat(timespec='seconds')}_\n\n")
        f.write(f"**Root:** `{root}`  \n**Total Findings:** {len(issues)}\n\n")
        if not issues:
            f.write("✅ No issues found.\n")
            return
        f.write("| File | Line | Kind | Value | Suggestion |\n|---|---:|---|---|---|\n")
        for i in sorted(issues, key=lambda x: (str(x.file), x.line)):
            rel = str(i.file.relative_to(root))
            f.write(f"| `{rel}` | {i.line} | {i.kind} | `{i.value}` | `{i.suggestion}` |\n")
    print(f"[OK] Wrote reduced audit: {len(issues)} findings -> {report_path}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True)
    p.add_argument("--config", default=None)
    p.add_argument("--report", default="..\\logs\\magic_number_audit_step5.md")
    args = p.parse_args()

    root = Path(args.root).resolve()
    config_values = load_existing_config_values(Path(args.config)) if args.config else {}

    excluded = {"magic_number_audit.py", "magic_number_audit_step5.py", "step_99_ScratchPad.py", "utils.py"}
    issues: List[Issue] = []

    for path in root.rglob("*.py"):
        if path.name in excluded or any(part in ("venv", ".venv", "archive", "__pycache__", "tests", ".ipynb_checkpoints") for part in path.parts):
            continue
        if re.search(r"step_0[7-9]|step_10", path.name):
            continue  # skip post-Step 5 work
        try:
            issues.extend(scan_file(path, config_values))
        except Exception:
            pass

    write_report(issues, Path(args.report).resolve(), root)

if __name__ == "__main__":
    main()