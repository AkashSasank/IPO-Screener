#!/usr/bin/env zsh
set -euo pipefail

# Create and activate a virtual environment, install requirements.txt,
# then ensure Playwright and its browsers are installed.

ROOT_DIR="$(cd "$(dirname "${0}")/.." && pwd)"
REQ_FILE="$ROOT_DIR/requirements.txt"
VENV_DIR="$ROOT_DIR/.venv"

echo "Project root: $ROOT_DIR"

if [ ! -f "$REQ_FILE" ]; then
  echo "ERROR: requirements.txt not found at $REQ_FILE"
  exit 1
fi

if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtualenv at $VENV_DIR"
  python3 -m venv "$VENV_DIR"
fi

# shellcheck source=/dev/null
source "$VENV_DIR/bin/activate"

echo "Upgrading pip / setuptools / wheel..."
pip install --upgrade pip setuptools wheel

echo "Installing python packages from requirements.txt..."
pip install -r "$REQ_FILE"

# Ensure playwright package is installed and run the browser installer.
echo "Ensuring Playwright package and its browsers are installed..."

# Use a robust Python check for whether the 'playwright' package is importable.
# Older one-liner used importlib.util.find_spec which can raise AttributeError
# if a non-standard importlib is on sys.path (e.g. a local importlib.py). This
# multi-step check prefers importlib.util.find_spec, falls back to
# importlib.find_spec, and finally tries an actual import as a last resort.
if python - <<'PY'
import sys
try:
    import importlib
    spec = None
    util = getattr(importlib, 'util', None)
    if util is not None:
        try:
            spec = util.find_spec('playwright')
        except Exception:
            spec = None
    elif hasattr(importlib, 'find_spec'):
        try:
            spec = importlib.find_spec('playwright')
        except Exception:
            spec = None
    # Final fallback: try importing the package directly
    if spec is None:
        try:
            __import__('playwright')
            sys.exit(0)
        except Exception:
            sys.exit(1)
    sys.exit(0 if spec else 1)
except Exception:
    try:
        __import__('playwright')
        sys.exit(0)
    except Exception:
        sys.exit(1)
PY
then
  echo "Playwright package detected. Running browser installer..."
  python -m playwright install --with-deps || python -m playwright install
else
  echo "Playwright package not found. Installing playwright package and browsers..."
  pip install playwright
  python -m playwright install --with-deps || python -m playwright install
fi

echo "\nSetup complete. To use the virtualenv run:\n  source $VENV_DIR/bin/activate\n"
