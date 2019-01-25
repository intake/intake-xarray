function build(){
    conda install -c conda-forge conda-build jinja2 intake>=0.4.1 xarray>=0.11.2 zarr dask
    conda list
    conda build -c conda-forge ./conda
}

build