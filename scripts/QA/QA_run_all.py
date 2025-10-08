"""
QA_run_all.py
-------------------------------------------------------------
Unified QA Orchestrator

Runs all QA diagnostics in sequence:
    1️⃣  Scan for numeric "magic literals"
    2️⃣  Reconcile them with config.py constants
    3️⃣  Audit config constant usage

Adds timing, colorized status, and summary reporting.

Usage:
    python QA_run_all.py
"""

import subprocess
import time
from pathlib import Path

try:
    from colorama import Fore, Style, init
    init(autoreset=True)
except Exception:
    class _Dummy:
        RED = GREEN = YELLOW = CYAN = RESET_ALL = ""
    Fore = Style = _Dummy()


def run_step(label: str, cmd: list[str]):
    """Run a subprocess command and time it."""
    print(f"\n{Fore.CYAN}{label}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}→ {' '.join(cmd)}{Style.RESET_ALL}")
    start = time.time()

    try:
        subprocess.run(cmd, check=True)
        duration = time.time() - start
        print(f"{Fore.GREEN}✔ Success{Style.RESET_ALL} ({duration:.1f}s)")
        return True, duration
    except subprocess.CalledProcessError as e:
        duration = time.time() - start
        print(f"{Fore.RED}✖ Failed{Style.RESET_ALL} ({duration:.1f}s, exit {e.returncode})")
        return False, duration


def main():
    here = Path(__file__).resolve().parent
    scripts_dir = here.parent
    config_path = scripts_dir / "config.py"
    audit_reports = here / "audit_reports"
    audit_reports.mkdir(parents=True, exist_ok=True)

    steps = [
        ("🔍 Step 1: Scanning for magic literals",
         ["python", str(here / "QA_scan_magic_literals.py"),
          "--scripts-dir", str(scripts_dir),
          "--output-dir", str(audit_reports)]),

        ("🧩 Step 2: Reconciling numeric constants",
         ["python", str(here / "QA_config_reconciler_numbers.py"),
          "--scripts-dir", str(scripts_dir),
          "--output-dir", str(audit_reports),
          "--config-file", str(config_path)]),

        ("📊 Step 3: Auditing config constant usage",
         ["python", str(here / "QA_config_usage_audit.py"),
          "--scripts-dir", str(scripts_dir),
          "--output-dir", str(audit_reports),
          "--config-file", str(config_path)]),
    ]

    total_start = time.time()
    results = []

    for label, cmd in steps:
        ok, duration = run_step(label, cmd)
        results.append((label, ok, duration))
        if not ok:
            print(f"\n{Fore.RED}⚠ QA halted — fix the issue above before rerunning.{Style.RESET_ALL}")
            break

    total_time = time.time() - total_start

    print("\n" + "=" * 70)
    print(f"{Fore.CYAN}QA RUN SUMMARY{Style.RESET_ALL}")
    print("-" * 70)
    for label, ok, duration in results:
        color = Fore.GREEN if ok else Fore.RED
        status = "PASSED" if ok else "FAILED"
        print(f"{color}{status:<7}{Style.RESET_ALL} {label:<45} {duration:>6.1f}s")
    print("-" * 70)
    print(f"Total elapsed time: {Fore.YELLOW}{total_time:.1f}s{Style.RESET_ALL}")
    print("=" * 70)

    if all(ok for _, ok, _ in results):
        print(f"\n{Fore.GREEN}✅ All QA checks completed successfully!{Style.RESET_ALL}")
    else:
        print(f"\n{Fore.RED}❌ Some QA steps failed — please review logs above.{Style.RESET_ALL}")


if __name__ == "__main__":
    main()
