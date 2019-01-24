function install() {
    conda update conda
    conda config --set auto_update_conda off --set always_yes yes --set verbosity 1
    conda config --add channels conda-forge
    conda install conda-build jinja2 pyyaml
    create -n intake python=3.6 $(python scripts/deps.py)
}

install