import os, sys
_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_CURRENT_DIR)
for path in (_CURRENT_DIR, _PROJECT_ROOT):
    if path not in sys.path:
        sys.path.append(path)
