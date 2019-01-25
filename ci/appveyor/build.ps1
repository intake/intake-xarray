function build(){
    conda install -c conda-forge conda-build jinja2 intake>=0.11.2 xarray>=0.4.2 zarr
    conda list
    conda build -c conda-forge ./conda
}

build