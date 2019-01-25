function install() {
    conda update --yes conda
    conda config --set auto_update_conda off --set always_yes yes --set changeps1 no --set show_channel_urls true

}

install