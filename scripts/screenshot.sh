#!/usr/bin/env bash
# Takes docs/dashboard.png of the dashboard (streamlit run app/dashboard.py must be running).
# Uses the same Playwright container approach as garmin-pulse, so nothing is installed on C:.
set -e
cd "$(dirname "$0")/.."
mkdir -p docs
MSYS_NO_PATHCONV=1 docker run --rm \
  -v "$(pwd -W)/docs:/out" \
  -v "$(pwd -W)/scripts:/scripts:ro" \
  mcr.microsoft.com/playwright/python:v1.49.0-noble \
  bash -c "pip install -q --break-system-packages playwright==1.49.0 && python /scripts/screenshot_playwright.py"
ls -la docs/dashboard.png
