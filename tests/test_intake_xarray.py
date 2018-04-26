
import numpy as np
import pytest
import xarray as xr

from .util import TEST_URLPATH, source, dataset   # noqa
from intake_xarray.netcdf import NetCDFSource


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
    source = NetCDFSource(TEST_URLPATH, chunks={'lon': 2})
    ds = source.read_chunked()
    dataset = xr.open_dataset(TEST_URLPATH, chunks={'lon': 2})

    assert ds.temp.chunks == dataset.temp.chunks


def test_read_partition(source):
    with pytest.raises(NotImplementedError):
        source.read_partition(None)


def test_to_dask(source, dataset):
    ds = source.to_dask()

    assert ds.dims == dataset.dims
    assert np.all(ds.temp == dataset.temp)
    assert np.all(ds.rh == dataset.rh)
