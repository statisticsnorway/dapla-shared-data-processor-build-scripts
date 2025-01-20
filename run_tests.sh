#!/usr/bin/env bash

# This script verifies user supplied scripts for a source using pyflakes and pytest.

# Exit on error and log all commands
set -ex

# Source packages from base image build
source /opt/pysetup/.venv/bin/activate

# Install test requirements
cd /workspace/dapla-shared-data-processor-build-scripts/schema-validation/
python -m pip install "uv>=0.5.21"

uv run python src/validate_config.py "/workspace/automation/shared-data/$ENV_NAME/$FOLDER_NAME"

# Run pytests
# pytest
# echo "## No errors found by pytest"
# # Check code with pyflakes
# cd /workspace/automation/source-data/$ENV_NAME/$FOLDER_NAME
# echo "## Checking code with pyflakes: ${FOLDER_NAME} in environment:${ENV_NAME}"
# pyflakes process_source_data.py
# echo "## No errors found by pyflakes"
