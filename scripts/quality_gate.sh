#!/bin/bash
# Check the published Kaggle quality indicators against their thresholds.
#
# Used by the daily quality gate workflow and by local_test.sh after the
# system test publishes, so both run the exact same check. Thresholds come
# from the QUALITY_* environment variables (see managers/quality_indicators.py).
# Extra arguments go to scripts/kaggle_quality_gate.py.
set -euo pipefail
cd "$(dirname "$0")/.."
exec python scripts/kaggle_quality_gate.py "$@"
