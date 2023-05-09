# -*- coding: utf-8 -*-
import numpy as np
import os
import pytest

from intake import open_catalog


@pytest.fixture
def catalog1():
    path = os.path.dirname(__file__)
    return open_catalog(os.path.join(path, 'data', 'catalog.yaml'))


def test_catalog(catalog1, dataset):
    source = catalog1['xarray_source'].get()
    ds = source.read()

    assert ds.dims == dataset.dims
    assert np.all(ds.temp == dataset.temp)
    assert np.all(ds.rh == dataset.rh)


def test_persist(catalog1):
    from intake_xarray import ZarrSource
    source = catalog1['blank']
    s2 = source.persist()
    assert source.has_been_persisted
    assert isinstance(s2, ZarrSource)
    assert s2.is_persisted
    assert (source.read() == s2.read()).all()


def test_import_error(mock_import_xarray, catalog1):
    s = catalog1['xarray_source']()  # this is OK
    with pytest.raises(ImportError):
        s.discover()
