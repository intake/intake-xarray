# -*- coding: utf-8 -*-

import numpy as np
import pytest
import xarray as xr

from .util import TEST_URLPATH, cdf_source, zarr_source, dataset  # noqa


@pytest.mark.parametrize('source', ['cdf', 'zarr'])
def test_discover(source, cdf_source, zarr_source, dataset):
    source = {'cdf': cdf_source, 'zarr': zarr_source}[source]
    r = source.discover()

    assert r['datashape'] is None
    assert r['dtype'] is xr.Dataset
    assert r['metadata'] is not None

    assert source.datashape is None
    assert source.metadata['dims'] == dict(dataset.dims)
    assert set(source.metadata['data_vars']) == set(dataset.data_vars.keys())
    assert set(source.metadata['coords']) == set(dataset.coords.keys())


@pytest.mark.parametrize('source', ['cdf', 'zarr'])
def test_read(source, cdf_source, zarr_source, dataset):
    source = {'cdf': cdf_source, 'zarr': zarr_source}[source]

    ds = source.read_chunked()
    assert ds.temp.chunks

    ds = source.read()
    assert ds.dims == dataset.dims
    assert np.all(ds.temp == dataset.temp)
    assert np.all(ds.rh == dataset.rh)


@pytest.mark.parametrize('source', ['cdf', 'zarr'])
def test_read_partition(source, cdf_source, zarr_source):
    source = {'cdf': cdf_source, 'zarr': zarr_source}[source]
    with pytest.raises(NotImplementedError):
        source.read_partition(None)


@pytest.mark.parametrize('source', ['cdf', 'zarr'])
def test_to_dask(source, cdf_source, zarr_source, dataset):
    source = {'cdf': cdf_source, 'zarr': zarr_source}[source]
    ds = source.to_dask()

    assert ds.dims == dataset.dims
    assert np.all(ds.temp == dataset.temp)
    assert np.all(ds.rh == dataset.rh)
