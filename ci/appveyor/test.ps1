function test() {
    conda env create -n test_env --file ci/environment-py36.yml
    source activate test_env
    conda list
    pip install --no-deps -e .
    pytest --verbose
}

test