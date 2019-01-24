function build(){
    conda build -c defaults -c conda-forge --no-test ./conda
    conda install --use-local intake-xarray
    conda install -c conda-forge -c defaults netcdf4 rasterio pytest scikit-image
    conda list
}

build