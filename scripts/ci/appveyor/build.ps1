function build(){
    conda build -c conda-forge -c defaults --no-test ./conda
    conda install -c conda-forge -c defaults --use-local intake-xarray
    conda install -c conda-forge -c defaults netcdf4 rasterio pynio pytest scikit-image
    conda list
}

build