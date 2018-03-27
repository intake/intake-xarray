
from dask.dataframe.core import DataFrame
import numpy as np
import os
import pytest
import pandas as pd
import time
import xarray as xr

from .util import source, dataset, TEST_URLPATH

from intake_netcdf import NetCDFPlugin, NetCDFSource 

def test_discover(source, dataset):
    r = source.discover()

    assert r['datashape'] is None
    assert r['dtype'] is xr.Dataset
    assert r['metadata'] is not None

    assert source.datashape is None
    assert source.metadata['dims'] == dict(dataset.dims)
    assert source.metadata['data_vars'] == tuple(dataset.data_vars.keys())
    assert source.metadata['coords'] == tuple(dataset.coords.keys())

def test_read(source, dataset):
    ds = source.read()

    assert ds.dims == dataset.dims
    assert np.all(ds.temp == dataset.temp)
    assert np.all(ds.rh == dataset.rh)

def test_read_chunked():
    source = NetCDFSource(TEST_URLPATH, xarray_kwargs={'chunks': {'lon': 2}})
    ds = source.read()
    dataset = xr.open_dataset(TEST_URLPATH, chunks={'lon': 2})

    assert ds.temp.chunks == dataset.temp.chunks

def test_to_dask(source, dataset):
    df = source.to_dask()
    assert type(df) == DataFrame
