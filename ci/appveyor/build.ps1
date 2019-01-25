function build(){
    conda install -c conda-forge conda-build conda-verify jinja2 intake>=0.4.1 xarray>=0.11.0 zarr dask netcdf4
    conda list
    conda build -c conda-forge ./conda
}

build