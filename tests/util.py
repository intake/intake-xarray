# -*- coding: utf-8 -*-

import os
import pytest
import xarray as xr

from intake_xarray.netcdf import NetCDFSource

TEST_DATA_DIR = 'tests/data'
TEST_DATA = 'example_1.nc'
TEST_URLPATH = os.path.join(TEST_DATA_DIR, TEST_DATA)


@pytest.fixture
def source():
    return NetCDFSource(TEST_URLPATH, {})


@pytest.fixture
def dataset():
    return xr.open_dataset(TEST_URLPATH)
