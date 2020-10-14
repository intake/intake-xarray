import intake
import os
import pytest
import requests
import subprocess
import time
import xarray as xr
import fsspec

PORT = 8425 # for intake-server tests
here = os.path.abspath(os.path.dirname(__file__))
cat_file = os.path.join(here, 'data', 'catalog.yaml')
DIRECTORY = os.path.join(here, 'data')


@pytest.fixture(scope='module')
def data_server():
    ''' Serves test/data folder to http://localhost:8000 '''
    pwd = os.getcwd()
    os.chdir(DIRECTORY)
    command = ['python', '-m', 'RangeHTTPServer']
    try:
        P = subprocess.Popen(command)
        timeout = 10
        while True:
            try:
                requests.get('http://localhost:8000')
                break
            except:
                time.sleep(0.1)
                timeout -= 0.1
                assert timeout > 0
        yield 'http://localhost:8000'
    finally:
        os.chdir(pwd)
        P.terminate()
        P.communicate()


def test_list(data_server):
    h = fsspec.filesystem("http")
    out = h.glob(data_server + '/')
    assert len(out) > 0
    assert data_server+'/RGB.byte.tif' in out


def test_open_rasterio(data_server):
    url = f'{data_server}/RGB.byte.tif'
    source = intake.open_rasterio(url, chunks={})
    da = source.to_dask()
    assert isinstance(da, xr.core.dataarray.DataArray)


# Remote catalogs with intake-server
@pytest.fixture(scope='module')
def intake_server():
    command = ['intake-server', '-p', str(PORT), cat_file]
    try:
        P = subprocess.Popen(command)
        timeout = 10
        while True:
            try:
                requests.get('http://localhost:{}'.format(PORT))
                break
            except:
                time.sleep(0.1)
                timeout -= 0.1
                assert timeout > 0
        yield 'intake://localhost:{}'.format(PORT)
    finally:
        P.terminate()
        P.communicate()


def test_remote_netcdf(intake_server):
    cat_local = intake.open_catalog(cat_file)
    cat = intake.open_catalog(intake_server)
    assert 'xarray_source' in cat
    source = cat.xarray_source()
    assert isinstance(source._ds, xr.Dataset)
    assert source._schema is None
    source._get_schema()
    assert source._schema is not None
    repr(source.to_dask())
    assert (source.to_dask().rh.data.compute() ==
            cat_local.xarray_source.to_dask().rh.data.compute()).all()
    assert (source.read() ==
            cat_local.xarray_source.read()).all()


def test_remote_tiff(intake_server):
    pytest.importorskip('rasterio')
    cat_local = intake.open_catalog(cat_file)
    cat = intake.open_catalog(intake_server)
    assert 'tiff_source' in cat
    source = cat.tiff_source()
    assert isinstance(source._ds, xr.Dataset)
    assert source._schema is None
    source._get_schema()
    assert source._schema is not None
    repr(source.to_dask())
    remote = source.to_dask().data.compute()
    local = cat_local.tiff_source.to_dask().data.compute()
    assert (remote == local).all()
    assert (source.read() ==
            cat_local.xarray_source.read()).all()
