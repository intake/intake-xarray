function install() {
    conda config --set auto_update_conda off
    conda config --set always_yes yes
    conda config --add channels conda-forge
    conda config --get channels
    conda config --set verbosity 3
    conda create -n intake python=3.6 conda-build jinja2 pyyaml pytest $(python scripts/deps.py)
}

install