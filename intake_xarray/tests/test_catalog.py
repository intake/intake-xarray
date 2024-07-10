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


def test_import_error(mock_import_xarray, catalog1):
    s = catalog1['xarray_source']()  # this is OK
    with pytest.raises(ImportError):
        s.discover()
