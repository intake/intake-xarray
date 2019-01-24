function build(){
    conda activate intake
    conda list
    conda build -c conda-forge -c defaults --no-test ./conda
    conda install --use-local intake-xarray netcdf4 rasterio pytest scikit-image
    conda list
}

build