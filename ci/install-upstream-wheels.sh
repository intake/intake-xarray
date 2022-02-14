#!/usr/bin/env bash

conda uninstall -y --force \
    aiohttp \
    flask \
    h5netcdf \
    netcdf4 \
    pydap \
    rasterio \
    tornado \
    scikit-image \
    zarr \
    s3fs \
    moto
# to limit the runtime of Upstream CI
python -m pip install pytest-timeout
python -m pip install \
    --no-deps \
    --upgrade \
    git+https://github.com/aio-libs/aiohttp \
    git+https://github.com/boto/boto3 \
    git+https://github.com/pallets/flask \
    git+https://github.com/h5netcdf/h5netcdf \
    git+https://github.com/Unidata/netcdf4-python \
    git+https://github.com/pydap/pydap \
    git+https://github.com/rasterio/rasterio \
    git+https://github.com/scikit-image/scikit-image \
    git+https://github.com/zarr-developers/zarr-python \
    git+https://github.com/fsspec/s3fs \
    git+https://github.com/spulec/moto
