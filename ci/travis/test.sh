#!/bin/bash
set -e # exit on error

echo "Creating test env"
conda env create -n test_env --file ci/environment-${CONDA_ENV}.yml
source activate test_env
conda list

echo "Installing intake_xarray."
pip install --no-deps -e .

echo "Running tests"
pytest --verbose
