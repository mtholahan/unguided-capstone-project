## 🧠 GPT Suggestion: How to Use It

1. Run:

   ```powershell
   python scan_magic_literals.py
   ```

   You’ll get:

   ```
   📊 Audit complete → ...\magic_numbers_audit.csv
   🧱 JSON audit data → ...\magic_literals.json
   ```

2. Then run:

   ```powershell
   python config_reconciler_numbers.py
   ```

   This will use the same JSON and produce your `config_mapping.csv` + `config_suggestions.py`.

------

## 🧾 Example Summary Output

```
🔍 Scanning for numeric magic literals...
🧭 Found 11 Step scripts to audit:
   • step_03b_rehydrate_guids.py
   • step_04_mb_full_join.py
   ...
📊 Audit complete → ...\magic_numbers_audit.csv
🧱 JSON audit data → ...\magic_literals.json

📈 Summary:
   threshold       6 found
   timing          3 found
   row limit       2 found
   retry           1 found
   unknown         2 found

💡 Tip: Focus first on 'threshold', 'limit', and 'timing' kinds.
```

## 🧠 GPT Suggestion

This pairing —
 `scan_magic_literals.py` + `config_reconciler_numbers.py` —
 is now your **final, production-grade “magic number pipeline.”**

✅ Next time you want a configuration audit, it’s just:

```
python scan_magic_literals.py
python config_reconciler_numbers.py
```





### 🧭 How to Run

From PowerShell, while in your `scripts` folder:

```
python analyze_suggestions_clusters.py
```

------

### 📈 Example Output

```
📊 Constant Value Clusters:
                         count     min      max     mean  median
FUZZ_THRESHOLD           12.0    0.3    13.0     3.992    4.0
ROW_LIMIT                5.0     0.02 1000000.0  200205.0  20.0
MAGIC_CONST              8.0     2.0   2025.0     623.5    40.0
API_THROTTLE_SECONDS     2.0     0.25  1.5        0.875    0.875
TIMEOUT_SECONDS          1.0    10.0   10.0       10.0    10.0

⚠️ Notable Variance — worth reviewing for consolidation:
   ROW_LIMIT                 range = 999999.98
   MAGIC_CONST               range = 2023.0
   FUZZ_THRESHOLD            range = 12.7
```

------

### 🧠 What This Tells You

| Insight Type               | Meaning                                                      |
| -------------------------- | ------------------------------------------------------------ |
| **Low spread** (min ≈ max) | These constants are stable — good candidates for immediate config promotion |
| **High spread**            | Indicates inconsistency — review whether those values should be unified |
| **Outliers**               | Help you spot mis-scaled values (like a `0.001` threshold vs `10.0`) |

------

🧠 **GPT Suggestion:**
 Once you’ve identified constants you *do* want to unify (say all `FUZZ_THRESHOLD_VAL` ≈ 4–6),
 you can create a short mapping in `config.py` like:

```
FUZZ_THRESHOLD_DEFAULT = 5.0
FUZZ_THRESHOLD_LENIENT = 10.0
```

and have your Step scripts import accordingly.





