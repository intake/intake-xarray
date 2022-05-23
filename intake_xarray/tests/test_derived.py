import os
import pytest

from intake import open_catalog
from xarray.tests import assert_allclose


# Function used in xarray_source_sel entry in catalog.yaml
def _sel(ds, indexers: str):
    """indexers: dict (stored as str) which is passed to xarray.Dataset.sel"""
    return ds.sel(eval(indexers))


@pytest.fixture
def catalog():
    path = os.path.dirname(__file__)
    return open_catalog(os.path.join(path, "data", "catalog.yaml"))


def test_catalog(catalog):
    expected = catalog["xarray_source"].read().sel(lat=20)
    actual = catalog["xarray_source_sel"].read()
    assert_allclose(actual, expected)
