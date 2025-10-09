## 🧭 Step 1 – Launch VS Code in the Right Folder

From **PowerShell** or **Command Prompt**:

```bash
cd C:\Projects\unguided-capstone-project
# Load environment variables
.\setup_env.ps1
# Activate the virtual environment
.\.venv\Scripts\Activate.ps1
# Now spawn Visual Code
code .
```



------

## ⚙️ Step 2 – Verify the Python Environment

Once VS Code opens:

1. Press **Ctrl + Shift + P** (Command Palette).

2. Type and select → **Python: Select Interpreter**.

3. Choose your virtual environment:

   ```
   .venv\Scripts\python.exe
   ```

   (If you don’t see it, click *Enter interpreter path* → *Find…* → browse to
    `C:\Projects\unguided-capstone-project\.venv\Scripts\python.exe`.)

✅ You’ll see it appear in the lower-right or status bar like:
 `Python 3.xx ('.venv': venv)`

------

## 🧩 Step 3 – Open the Scripts Directory

In the **Explorer panel (Ctrl + Shift + E)**:

- Navigate to `scripts/`

- You should see all your files, e.g.:

  ```
  step_05_*.py
  step_06_fetch_tmdb.py
  step_07_prepare_tmdb_input.py
  step_08_match_tmdb.py
  test_step_08_comparison.py
  ```

Click any of them to edit.

------

## ▶️ Step 4 – Run a Script in VS Code’s Terminal

You can execute your steps directly from the integrated terminal:

```
python scripts\step_06_fetch_tmdb.py
```

or if you’re already inside the `scripts/` folder:

```
python step_06_fetch_tmdb.py
```

*(Make sure the terminal is using your virtual environment — it should say `(.venv)` before the prompt.)*

### 🧩 How “resume” works in your current `main.py`

Inside the `main()` function:

```
parser.add_argument("--resume", type=str, default=None,
                    help="Step number to resume from (e.g., '05' to start at Step05FilterSoundtracks)")
```

The script then:

1. Builds the ordered list of step objects via `build_steps()`.
2. Scans for the step whose **numeric ID** matches your `--resume` argument (like `"05"` or `"08"`).
3. Starts execution *from that index onward*.

So, if you run:

```
python main.py --resume 08
```

it will start with:

```
▶ Resuming pipeline at Step 08: Match TMDb (Instrumented)
```

and then run:

- ### Step 08

- Step 09

- Step 10

- Step 10B

...and then finish by printing your summary.

------

✅ Accepted values for `--resume`

You can provide **either the numeric part** (`"08"`, `"9"`, etc.)
 or the full string like `"Step08"`, `"Step 08"`, `"08:"`, etc.
 They’re normalized by the code:

```
target = args.resume.strip().upper().replace("STEP", "").replace(":", "")
```

and then compared to each step’s `.name`.

So all of these are equivalent:

```
python main.py --resume 08
python main.py --resume step08
python main.py --resume "Step 08"
python main.py --resume 8
```

------

### ⚙️ Important Practical Tips

| Case                          | What Happens                                                 |
| ----------------------------- | ------------------------------------------------------------ |
| **No `--resume` argument**    | Entire pipeline runs from Step 00.                           |
| **Bad `--resume` value**      | You’ll get: `❌ Invalid resume step` and a list of valid IDs. |
| **File dependencies missing** | If an earlier step’s output is missing, the resumed step may fail — so make sure required intermediate files exist. |
| **New Step 08 replacement**   | Since you fixed `main.py` to point to `step_08_match_instrumented`, `--resume 08` will now correctly call the instrumented version. |

------

## 🧱 Step 5 – Optional Quality of Life Setup

- **Auto-activate the venv**:
   Add this line to `.vscode/settings.json` in your project root:

  ```
  {
    "python.defaultInterpreterPath": ".venv\\Scripts\\python.exe"
  }
  ```

- **Install recommended VS Code extensions**:

  - *Python* (by Microsoft)
  - *Pylance*
  - *Jupyter* (for notebook analysis)
  - *Code Runner* (if you want one-click runs)

------

## ✅ After Setup

You’ll be able to:

- Edit and run all your pipeline scripts,
- See syntax highlighting and linting from your `.venv`,
- And open `step09_analysis.ipynb` (once we create it) directly in the built-in Jupyter interface.





