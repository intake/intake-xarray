#!/bin/bash
set -e # exit on error
set -x

echo "Creating test env"
conda env create -n test_env --file ci/environment-${CONDA_ENV}.yml
source activate test_env
# dev versions
pip install git+https://github.com/intake/filesystem_spec --no-deps
pip install git+https://github.com/intake/intake --no-deps
conda list

echo "Installing intake_xarray."
pip install --no-deps -e .

echo "Running tests"
pytest --verbose
