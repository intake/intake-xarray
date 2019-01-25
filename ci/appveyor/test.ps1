function test() {
    conda env create -n test_env --file ci/environment-${CONDA_ENV}.yml
    conda activate test_env
    conda list
    pip install --no-deps -e .
    pytest --verbose
}

test