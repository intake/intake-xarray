#!/bin/bash

set -e # exit on error

echo "Activating environment"
conda activate intake
conda list

echo "Building conda package."
conda build -c conda-forge -c defaults --no-test ./conda

# If tagged, upload package to main channel, otherwise, run tests
if [ -n "$TRAVIS_TAG" ]; then
    echo "Uploading conda package."
    anaconda -t ${ANACONDA_TOKEN} upload -u intake --force `conda build --output ./conda`
else
    echo "Installing conda package locally."
    conda install --use-local intake-xarray netcdf4 rasterio pynio pytest scikit-image
    conda list

    echo "Running unit tests."
    py.test
fi