#!/usr/bin/env python3
"""
magic_number_audit.py
Detects "magic numbers" and hard-coded paths/URLs in a Python codebase and proposes
config.py constants. Produces a Markdown report with context lines and suggestions.

Usage:
    python magic_number_audit.py --root C:\Projects\unguided-capstone-project\Scripts --config C:\Projects\unguided-capstone-project\Scripts\config.py --report reports\magic_number_audit.md

Notes:
- Uses Python AST for robust parsing and basic heuristics for context-aware suggestions.
- Safe by default: read-only. No in-place edits.
- Excludes config.py and common virtual env / test / archive folders.
"""

import argparse
import ast
import os
import re
import sys
import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# -----------------------------
# Helpers
# -----------------------------

def read_text(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        # Fall back to latin-1 if needed
        return p.read_text(encoding="latin-1")

def slugify(name: str) -> str:
    name = re.sub(r'[^A-Za-z0-9]+', '_', name).strip('_')
    if not name:
        name = "VALUE"
    return name.upper()

def get_func_name(node: ast.AST) -> Optional[str]:
    if isinstance(node, ast.Call):
        f = node.func
        if isinstance(f, ast.Name):
            return f.id
        if isinstance(f, ast.Attribute):
            return f.attr
    return None

def get_full_attr_name(node: ast.AST) -> str:
    # Convert an attribute chain to dotted path, e.g., time.sleep
    names = []
    cur = node
    while isinstance(cur, ast.Attribute):
        names.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        names.append(cur.id)
    return ".".join(reversed(names))

def is_module_level_all_caps_assign(node: ast.AST) -> bool:
    if not isinstance(node, ast.Assign):
        return False
    # Only consider simple Name targets at module level
    names = []
    for t in node.targets:
        if isinstance(t, ast.Name):
            names.append(t.id)
        elif isinstance(t, (ast.Tuple, ast.List)):
            for elt in t.elts:
                if isinstance(elt, ast.Name):
                    names.append(elt.id)
    return all(n.isupper() for n in names) if names else False

def looks_like_path(s: str) -> bool:
    if "/" in s or "\\" in s:
        return True
    if re.match(r"^[A-Za-z]:\\", s):
        return True
    return False

def looks_like_url(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://")

def looks_like_filename(s: str) -> bool:
    return bool(re.search(r"\.(csv|tsv|parquet|json|txt|log|bz2|gz|zip)$", s, re.IGNORECASE))

def load_existing_config_values(config_path: Optional[Path]) -> Dict[str, Any]:
    values: Dict[str, Any] = {}
    if not config_path or not config_path.exists():
        return values
    try:
        tree = ast.parse(read_text(config_path))
    except Exception:
        return values
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id.isupper():
                    try:
                        values[target.id] = ast.literal_eval(node.value)
                    except Exception:
                        pass
    return values

# -----------------------------
# Core Visitor
# -----------------------------

class Issue:
    def __init__(self, file: Path, line: int, col: int, value: Any, kind: str, context: str, suggestion: str):
        self.file = file
        self.line = line
        self.col = col
        self.value = value
        self.kind = kind  # "number" | "string"
        self.context = context
        self.suggestion = suggestion

    def key(self) -> Tuple[str, int, int, Any]:
        return (str(self.file), self.line, self.col, self.value)

class MagicFinder(ast.NodeVisitor):
    SMALL_RANGE_MAX_DEFAULT = 3
    ALLOWED_NUMERIC_LITERALS = {0, 1, -1}  # conservative

    def __init__(self, file_path: Path, code: str, parent_map: Dict[ast.AST, ast.AST],
                 config_values: Dict[str, Any], small_range_max: int = SMALL_RANGE_MAX_DEFAULT):
        self.file_path = file_path
        self.code = code
        self.lines = code.splitlines()
        self.parent_map = parent_map
        self.config_values = config_values
        self.small_range_max = small_range_max
        self.issues: List[Issue] = []
        # Quick reverse index of config literal values to names
        self.config_literals_to_names: Dict[Any, List[str]] = {}
        for name, val in config_values.items():
            self.config_literals_to_names.setdefault(val, []).append(name)

    def add_issue(self, node: ast.AST, value: Any, kind: str, context: str, suggestion: str):
        line = getattr(node, "lineno", 1)
        col = getattr(node, "col_offset", 0)
        self.issues.append(Issue(self.file_path, line, col, value, kind, context, suggestion))

    def get_context_snippet(self, line_no: int) -> str:
        idx = max(0, line_no - 1)
        if 0 <= idx < len(self.lines):
            return self.lines[idx].rstrip("\n")
        return ""

    def parent(self, node: ast.AST) -> Optional[ast.AST]:
        return self.parent_map.get(node)

    def visit_Assign(self, node: ast.Assign):
        # Ignore module-level ALL_CAPS assignments (treated as config-like)
        if isinstance(self.parent(node), ast.Module) and is_module_level_all_caps_assign(node):
            return  # do not scan RHS here to avoid double-reporting constants meant to be constants
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant):
        val = node.value

        # Skip None, bools
        if isinstance(val, (type(None), bool)):
            return

        # Numbers
        if isinstance(val, (int, float)):
            if val in self.ALLOWED_NUMERIC_LITERALS:
                return

            # skip if this exact literal exists in config values
            if val in self.config_literals_to_names:
                return

            # Special-case: range small upper bound
            p = self.parent(node)
            if isinstance(p, ast.Call) and get_func_name(p) == "range":
                # find first positional arg if any
                args = p.args
                if node in args:
                    # Determine if it's the stop or start; treat any small literal as ok
                    if isinstance(val, int) and abs(val) <= self.small_range_max:
                        return
                # Otherwise, consider it an issue
                context = f"range(...) upper bound or arg"
                suggestion = "MAX_DEBUG_ROWS"
                self.add_issue(node, val, "number", context, suggestion)
                return

            # More context
            context, suggestion = self._context_for_number(node, val)
            self.add_issue(node, val, "number", context, suggestion)
            return

        # Strings
        if isinstance(val, str):
            # skip empty strings or single-char whitespace
            if not val or val.strip() == "":
                return

            # skip if this literal value exists in config
            if val in self.config_literals_to_names:
                return

            s = val
            pathish = looks_like_path(s)
            urlish = looks_like_url(s)
            fileish = looks_like_filename(s)

            if pathish or urlish or fileish:
                context = "hard-coded path" if pathish else "hard-coded URL" if urlish else "hard-coded filename"
                suggestion = self._suggest_for_string_literal(node, s)
                self.add_issue(node, s, "string", context, suggestion)
                return

        # default: ignore other constants
        return

    def _context_for_number(self, node: ast.AST, val: Any) -> Tuple[str, str]:
        # try to infer context
        p = self.parent(node)

        # Default suggestion based on surrounding code
        func = None
        kwarg_name = None

        if isinstance(p, ast.keyword):
            kwarg_name = p.arg
            func_node = self.parent(p)
            if isinstance(func_node, ast.Call):
                func = get_func_name(func_node)

        if isinstance(p, ast.Call) and func is None:
            func = get_func_name(p)

        if isinstance(p, ast.Compare):
            # threshold compare e.g. if score > 0.8
            lhs_name = None
            if isinstance(p.left, ast.Name):
                lhs_name = p.left.id
            op_names = [type(op).__name__ for op in p.ops]
            context = f"comparison ({' '.join(op_names)})"
            base = slugify(f"{lhs_name or 'THRESHOLD'}")
            if isinstance(val, float) and 0 < val < 1:
                # probable fraction/ratio
                suggestion = f"{base}_RATIO"
            elif isinstance(val, int):
                suggestion = f"{base}_THRESHOLD"
            else:
                suggestion = base
            return context, suggestion

        if isinstance(p, ast.keyword):
            # A keyword arg -> map common names
            mapping = {
                "chunksize": "CHUNK_SIZE",
                "timeout": "REQUEST_TIMEOUT_SECS",
                "retries": "RETRY_LIMIT",
                "max_retries": "RETRY_LIMIT",
                "limit": "ROW_LIMIT",
                "n": "TOP_N",
                "k": "TOP_K",
                "page": "TMDB_PAGE_LIMIT",
                "per_page": "TMDB_PAGE_SIZE",
                "size": "BATCH_SIZE",
                "max_workers": "MAX_WORKERS",
                "sleep": "SLEEP_SECONDS",
            }
            suggestion = mapping.get(kwarg_name or "", slugify(kwarg_name or "CONST"))
            context = f"keyword arg: {kwarg_name} in {func or 'call'}"
            return context, suggestion

        if isinstance(p, ast.Call):
            # emitted from positional args; check for known functions
            full_name = None
            if isinstance(p.func, ast.Attribute):
                full_name = get_full_attr_name(p.func)
            elif isinstance(p.func, ast.Name):
                full_name = p.func.id

            known_func_to_const = {
                "time.sleep": "SLEEP_SECONDS",
                "sleep": "SLEEP_SECONDS",
                "range": "MAX_RANGE",
                "round": "ROUND_DIGITS",
                "pd.read_csv": "CSV_READ_CHUNK_SIZE",
                "read_csv": "CSV_READ_CHUNK_SIZE",
                "head": "TOP_N",
                "sample": "SAMPLE_FRAC" if isinstance(val, float) and 0 < val < 1 else "SAMPLE_N",
            }
            suggestion = known_func_to_const.get(full_name or "", "CONST_VALUE")
            context = f"positional arg in call: {full_name or func or 'call'}"
            return context, suggestion

        if isinstance(p, (ast.BinOp, ast.AugAssign)):
            # multiplication (e.g., 1000*60), divisions etc.
            context = "arithmetic expression"
            if isinstance(val, int) and val in (60, 1000, 1024):
                suggestion = "SECS_PER_MIN" if val == 60 else "MS_PER_SECOND" if val == 1000 else "BYTES_PER_KB"
            else:
                suggestion = "ARITH_CONST"
            return context, suggestion

        # fallback
        return "literal number", "CONST_VALUE"

    def _suggest_for_string_literal(self, node: ast.AST, s: str) -> str:
        s_lower = s.lower()
        # If path includes tmdb/musicbrainz, tailor the name
        if "tmdb" in s_lower:
            base = "TMDB"
        elif "musicbrainz" in s_lower or "mbdump" in s_lower or "mb" in s_lower:
            base = "MB"
        else:
            base = "PATH"

        # Filename-aware
        m = re.search(r"([A-Za-z0-9_\-]+\.(csv|tsv|parquet|json|txt|bz2|gz|zip))", s)
        if m:
            fname = slugify(m.group(1).replace('.', '_'))
            return f"{base}_{fname}"

        # URL-aware
        if looks_like_url(s):
            if "themoviedb" in s_lower:
                return "TMDB_API_BASE"
            if "musicbrainz" in s_lower:
                return "MB_API_BASE"
            return "API_BASE_URL"

        # General path fallback
        parts = [p for p in re.split(r"[\\/]|\\\\", s) if p]
        if parts:
            hint = slugify(parts[-1])
            return f"{base}_{hint}"
        return f"{base}_DIR"

def build_parent_map(node: ast.AST) -> Dict[ast.AST, ast.AST]:
    parent_map: Dict[ast.AST, ast.AST] = {}
    for child in ast.walk(node):
        for kid in ast.iter_child_nodes(child):
            parent_map[kid] = child
    return parent_map

def scan_file(file_path: Path, config_values: Dict[str, Any], small_range_max: int) -> List[Issue]:
    try:
        code = read_text(file_path)
        tree = ast.parse(code)
    except Exception as e:
        # Skip files that fail to parse
        return []
    parent_map = build_parent_map(tree)
    finder = MagicFinder(file_path, code, parent_map, config_values, small_range_max=small_range_max)
    finder.visit(tree)
    # attach snippets into context? We'll add later in report writer
    return finder.issues

def write_report(issues: List[Issue], report_path: Path, root: Path):
    # Aggregate suggestions for "proposed config" section
    suggestions: Dict[str, List[Issue]] = {}
    for issue in issues:
        suggestions.setdefault(issue.suggestion, []).append(issue)

    # sort issues by file/line
    issues.sort(key=lambda x: (str(x.file), x.line, x.col))

    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as f:
        f.write(f"# Magic Number & Hard-coded Literal Audit\n")
        f.write(f"_Generated: {datetime.datetime.now().isoformat(timespec='seconds')}_\n\n")
        f.write(f"**Root:** `{root}`  \n")
        f.write(f"**Total Findings:** {len(issues)}\n\n")

        # Proposed config section
        f.write("## Proposed Config Candidates\n")
        if not suggestions:
            f.write("No findings. 🎉\n\n")
        else:
            f.write("| Suggested Name | Occurrences | Example Files |\n")
            f.write("|---|---:|---|\n")
            for name, occ in sorted(suggestions.items(), key=lambda kv: (-len(kv[1]), kv[0])):
                files = sorted({str(i.file.relative_to(root)) for i in occ})
                sample_files = ", ".join(files[:3]) + (" ..." if len(files) > 3 else "")
                f.write(f"| `{name}` | {len(occ)} | {sample_files} |\n")
            f.write("\n")

        # Detailed findings
        f.write("## Detailed Findings\n")
        if not issues:
            f.write("None.\n")
        else:
            f.write("| File | Line | Kind | Value | Context | Suggestion |\n")
            f.write("|---|---:|---|---|---|---|\n")
            for i in issues:
                rel = str(i.file.relative_to(root)) if i.file.is_relative_to(root) else str(i.file)
                val_repr = repr(i.value).replace("|", "\\|")
                f.write(f"| `{rel}` | {i.line} | {i.kind} | {val_repr} | {i.context} | `{i.suggestion}` |\n")

        # Footer
        f.write("\n---\n")
        f.write("**Next steps:**\n")
        f.write("1. For each suggestion, add a constant in `config.py`.\n")
        f.write("2. Replace usages in the listed files and import from `config`.\n")
        f.write("3. Re-run this audit until findings reach an acceptable level.\n")

def main():
    parser = argparse.ArgumentParser(description="Find magic numbers and hard-coded literals in a Python codebase.")
    parser.add_argument("--root", type=str, required=True, help="Root folder to scan (e.g., Scripts)")
    parser.add_argument("--config", type=str, default=None, help="Path to config.py for excluding known constants")
    parser.add_argument("--report", type=str, default="reports/magic_number_audit.md", help="Markdown report output path")
    parser.add_argument("--small-range-max", type=int, default=3, help="Ignore range upper bounds <= this value")
    parser.add_argument("--exclude", type=str, nargs="*", default=["venv", ".venv", "__pycache__", "tests", "archive"],
                        help="Folder name patterns to exclude")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    if not root.exists():
        print(f"[ERR] Root path not found: {root}", file=sys.stderr)
        sys.exit(2)

    config_values = {}
    if args.config:
        config_values = load_existing_config_values(Path(args.config))

    issues: List[Issue] = []
    for path in root.rglob("*.py"):
        # Exclusions
        if any(part in args.exclude for part in path.parts):
            continue
        if path.name.lower() == "config.py":
            continue

        file_issues = scan_file(path, config_values, small_range_max=args.small_range_max)
        issues.extend(file_issues)

    report_path = Path(args.report).resolve()
    write_report(issues, report_path, root)
    print(f"[OK] Wrote report with {len(issues)} findings -> {report_path}")

if __name__ == "__main__":
    main()
