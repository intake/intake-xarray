function build(){
    conda build --no-test ./conda
    conda install --use-local intake-xarray
    conda install netcdf4 rasterio pynio pytest scikit-image
    conda list
}

build