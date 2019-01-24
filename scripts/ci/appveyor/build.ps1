function build(){
    conda build -c conda-forge -c defaults --no-test ./conda
    conda install --use-local intake-xarray
    conda install netcdf4 rasterio pytest scikit-image
    conda list
}

build