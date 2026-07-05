#!/usr/bin/env bash
set -euo pipefail

python3 -m venv .venv
. .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo
echo "Installed LocalShim runtime dependencies."
echo "Activate with:"
echo "  source .venv/bin/activate"
echo
echo "Try:"
echo "  python main.py --help"