# -*- coding: utf-8 -*-

import os
import pytest
import shutil
import tempfile
import xarray as xr

from intake_xarray.netcdf import NetCDFSource
from intake_xarray.xzarr import ZarrSource

TEST_DATA_DIR = 'tests/data'
TEST_DATA = 'example_1.nc'
TEST_URLPATH = os.path.join(TEST_DATA_DIR, TEST_DATA)


@pytest.fixture
def cdf_source():
    return NetCDFSource(TEST_URLPATH, {})


@pytest.fixture
def dataset():
    return xr.open_dataset(TEST_URLPATH)


@pytest.fixture(scope='module')
def zarr_source():
    pytest.importorskip('zarr')
    try:
        tdir = tempfile.mkdtemp()
        data = xr.open_dataset(TEST_URLPATH)
        data.to_zarr(tdir)
        yield ZarrSource(tdir)
    finally:
        shutil.rmtree(tdir)
